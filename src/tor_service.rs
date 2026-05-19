// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Tor hidden-service lifecycle management.
//!
//! Bootstraps an arti Tor client, launches a `.onion` service, and serves the
//! existing Axum router over it using raw hyper HTTP/1.1 connections.
//!
//! Enabled only when the crate is compiled with `--features tor`.

#[cfg(feature = "tor")]
pub use imp::*;

#[cfg(feature = "tor")]
mod imp {
    use crate::CancellationToken;
    use anyhow::{Context, Result};
    use arti_client::{
        TorClient, TorClientConfig, config::onion_service::OnionServiceConfigBuilder,
    };
    use axum::Router;
    use axum::body::Body;
    use futures::StreamExt;
    use hyper::body::Incoming;
    use hyper_util::rt::TokioIo;
    use serde::Serialize;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    use tokio::sync::RwLock;
    use tokio::time::{Duration, timeout};
    use tor_cell::relaycell::msg::Connected;
    use tower::ServiceExt;
    use tracing::{error, info, warn};

    /// Privacy-relevant metadata exposed by the `/tor-info` endpoint.
    #[derive(Clone, Serialize)]
    pub struct TorInfo {
        /// The `.onion` address (v3, 56-char base32 + `.onion`).
        pub onion_address: Option<String>,
        /// Whether the Tor client finished bootstrapping.
        pub bootstrapped: bool,
        /// Total streams accepted since startup.
        pub streams_accepted: u64,
        /// Currently active circuits (rendezvous connections).
        pub circuits_active: u64,
        /// Total circuits rejected by the rate limiter since startup.
        pub circuits_rejected: u64,
    }

    /// Shared state updated by the Tor service task, readable via `/tor-info`.
    #[derive(Clone)]
    pub struct TorState {
        inner: Arc<TorStateInner>,
    }

    struct TorStateInner {
        onion_address: RwLock<Option<String>>,
        bootstrapped: AtomicBool,
        streams_accepted: AtomicU64,
        circuits_active: AtomicU64,
        circuits_rejected: AtomicU64,
    }

    impl Default for TorState {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TorState {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(TorStateInner {
                    onion_address: RwLock::new(None),
                    bootstrapped: AtomicBool::new(false),
                    streams_accepted: AtomicU64::new(0),
                    circuits_active: AtomicU64::new(0),
                    circuits_rejected: AtomicU64::new(0),
                }),
            }
        }

        async fn set_onion_address(&self, addr: String) {
            *self.inner.onion_address.write().await = Some(addr);
        }

        fn set_bootstrapped(&self) {
            self.inner.bootstrapped.store(true, Ordering::Relaxed);
        }

        fn increment_streams(&self) {
            self.inner.streams_accepted.fetch_add(1, Ordering::Relaxed);
        }

        fn increment_circuits_active(&self) {
            self.inner.circuits_active.fetch_add(1, Ordering::Relaxed);
        }

        fn decrement_circuits_active(&self) {
            self.inner.circuits_active.fetch_sub(1, Ordering::Relaxed);
        }

        fn increment_circuits_rejected(&self) {
            self.inner.circuits_rejected.fetch_add(1, Ordering::Relaxed);
        }

        pub async fn info(&self) -> TorInfo {
            TorInfo {
                onion_address: self.inner.onion_address.read().await.clone(),
                bootstrapped: self.inner.bootstrapped.load(Ordering::Relaxed),
                streams_accepted: self.inner.streams_accepted.load(Ordering::Relaxed),
                circuits_active: self.inner.circuits_active.load(Ordering::Relaxed),
                circuits_rejected: self.inner.circuits_rejected.load(Ordering::Relaxed),
            }
        }
    }

    // ── Circuit-aware rate limiting ───────────────────────────────────────────

    /// Limits new circuit (rendezvous) connections to the hidden service.
    ///
    /// Two independent limits are enforced:
    /// - `max_concurrent`: hard cap on simultaneously active circuits.
    /// - `window_max` per `window_duration`: rate cap on new circuit creation.
    ///
    /// Both are enforced at the circuit level — the only meaningful identity
    /// available to a Tor hidden service, since client IPs are not visible.
    struct CircuitLimiter {
        max_concurrent: u64,
        active: AtomicU64,
        window_max: u64,
        window_duration: Duration,
        // Guarded by a std mutex; the lock is never held across an await point.
        window_start: std::sync::Mutex<std::time::Instant>,
        window_count: AtomicU64,
    }

    impl CircuitLimiter {
        fn new(max_concurrent: u64, window_max: u64, window_duration: Duration) -> Arc<Self> {
            Arc::new(Self {
                max_concurrent,
                active: AtomicU64::new(0),
                window_max,
                window_duration,
                window_start: std::sync::Mutex::new(std::time::Instant::now()),
                window_count: AtomicU64::new(0),
            })
        }

        /// Returns `true` and records the circuit if both limits allow it.
        fn try_acquire(&self) -> bool {
            if self.active.load(Ordering::Relaxed) >= self.max_concurrent {
                return false;
            }

            let now = std::time::Instant::now();
            let mut ws = self.window_start.lock().unwrap();
            if now.duration_since(*ws) >= self.window_duration {
                *ws = now;
                self.window_count.store(1, Ordering::Relaxed);
            } else {
                let prev = self.window_count.fetch_add(1, Ordering::Relaxed);
                if prev >= self.window_max {
                    return false;
                }
            }

            self.active.fetch_add(1, Ordering::Relaxed);
            true
        }

        fn release(&self) {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// RAII guard that releases the `CircuitLimiter` slot and decrements the
    /// active-circuit counter when a circuit task exits, even on panic.
    struct CircuitGuard {
        limiter: Arc<CircuitLimiter>,
        state: TorState,
    }

    impl Drop for CircuitGuard {
        fn drop(&mut self) {
            self.limiter.release();
            self.state.decrement_circuits_active();
        }
    }

    // ── Public entry point ────────────────────────────────────────────────────

    /// Start the Tor hidden service and serve `router` over it.
    ///
    /// `state` must be the same `TorState` that was injected into `router` via
    /// `create_router` so that `/tor-info` reflects live Tor metrics.
    /// Shuts down gracefully when `cancellation_token` fires.
    pub async fn start_tor_service(
        router: Router,
        cancellation_token: CancellationToken,
        state: TorState,
    ) -> Result<()> {
        tokio::spawn(async move {
            if let Err(e) = run_tor_service(router, cancellation_token, state).await {
                error!("Tor service terminated with error: {e:#}");
            }
        });

        Ok(())
    }

    async fn run_tor_service(
        router: Router,
        cancellation_token: CancellationToken,
        state: TorState,
    ) -> Result<()> {
        info!("Bootstrapping Tor client…");

        let tor_client = timeout(
            Duration::from_secs(120),
            TorClient::create_bootstrapped(TorClientConfig::default()),
        )
        .await
        .context("Tor bootstrap timed out after 120 s — check network/firewall")?
        .context("Failed to bootstrap Tor client")?;

        state.set_bootstrapped();
        info!("Tor client bootstrapped");

        let nickname = "quixote"
            .to_owned()
            .try_into()
            .context("Invalid onion service nickname")?;

        let config = OnionServiceConfigBuilder::default()
            .nickname(nickname)
            .build()
            .context("Failed to build onion service config")?;

        let (onion_service, mut rend_requests) = tor_client
            .launch_onion_service(config)
            .context("Failed to launch onion service")?;

        // The onion address is known immediately after launch (derived from the key).
        if let Some(name) = onion_service.onion_address() {
            let addr = name.to_string();
            state.set_onion_address(addr.clone()).await;
            info!("Tor onion service listening at http://{addr}");
        }

        // 64 concurrent circuits; at most 30 new circuits per 60-second window.
        let limiter = CircuitLimiter::new(64, 30, Duration::from_secs(60));

        let mut shutdown_rx = cancellation_token.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    warn!("Tor service shutting down");
                    break;
                }
                rend_req = rend_requests.next() => {
                    let Some(rend_req) = rend_req else {
                        warn!("Tor rendezvous stream ended");
                        break;
                    };

                    if !limiter.try_acquire() {
                        state.increment_circuits_rejected();
                        warn!(
                            circuits_active = limiter.active.load(Ordering::Relaxed),
                            circuits_rejected = state.inner.circuits_rejected.load(Ordering::Relaxed),
                            "Tor circuit rate-limited — dropping circuit"
                        );
                        // Dropping rend_req without accepting signals rejection to the client.
                        continue;
                    }
                    state.increment_circuits_active();

                    // Guard ensures limiter slot + active counter are released
                    // when the circuit task exits, regardless of how it exits.
                    let guard = CircuitGuard { limiter: limiter.clone(), state: state.clone() };

                    // Each RendRequest represents one client connecting to the
                    // onion service.  Accepting it yields a stream of individual
                    // TCP-like streams (StreamRequests) over the circuit.
                    let mut incoming = match rend_req.accept().await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to accept rendezvous request: {e}");
                            drop(guard);
                            continue;
                        }
                    };

                    let router = router.clone();
                    let state = state.clone();

                    tokio::spawn(async move {
                        let _guard = guard;
                        while let Some(stream_req) = incoming.next().await {
                            // Accept only RELAY_BEGIN (new TCP stream) requests;
                            // others (e.g. RELAY_BEGIN_DIR) are intentionally ignored.
                            let data_stream = match stream_req.accept(Connected::new_empty()).await {
                                Ok(ds) => ds,
                                Err(e) => {
                                    warn!("Failed to accept data stream: {e}");
                                    continue;
                                }
                            };

                            state.increment_streams();
                            let router = router.clone();

                            tokio::spawn(async move {
                                serve_http(data_stream, router).await;
                            });
                        }
                    });
                }
            }
        }

        // Dropping the OnionService tells arti to stop publishing the descriptor.
        drop(onion_service);
        Ok(())
    }

    /// Serve a single HTTP/1.1 connection over a Tor `DataStream`.
    async fn serve_http(stream: arti_client::DataStream, router: Router) {
        let io = TokioIo::new(stream);

        // Wrap the axum Router in a hyper service_fn.
        // We need to map the body type: hyper gives us `Incoming`, axum expects `Body`.
        let svc = hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
            let router = router.clone();
            async move {
                let (parts, body) = req.into_parts();
                let axum_req = hyper::Request::from_parts(parts, Body::new(body));
                // ServiceExt::oneshot handles poll_ready + call.
                // axum Router's error type is Infallible, which hyper accepts.
                router.oneshot(axum_req).await
            }
        });

        if let Err(e) = hyper::server::conn::http1::Builder::new()
            .serve_connection(io, svc)
            .await
        {
            // Connection reset / closed cleanly is expected; log at debug only.
            tracing::debug!("Tor HTTP connection ended: {e}");
        }
    }
}
