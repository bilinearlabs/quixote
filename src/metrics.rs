// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT
//
//! Prometheus metrics exporter.
use anyhow::Result;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{
        HeaderValue, StatusCode,
        header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE},
    },
    response::Response,
    routing::get,
};
use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};
use std::{net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;

/// Configuration for the Prometheus metrics server.
#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub address: String,
    pub port: u16,
    pub allow_origin: Option<String>,
}

#[derive(Clone, Default)]
pub struct MetricsHandle {
    inner: Option<Arc<MetricsInner>>,
}

#[derive(Clone)]
struct MetricsInner {
    registry: Registry,
    indexed_block: IntGaugeVec,
    chain_head_block: IntGaugeVec,
    allow_origin: Option<String>,
}

impl MetricsHandle {
    pub fn new(config: &MetricsConfig) -> Result<Self> {
        if !config.enabled {
            return Ok(Self { inner: None });
        }

        let registry = Registry::new_custom(Some("quixote".to_string()), None)?;

        let indexed_block = IntGaugeVec::new(
            Opts::new(
                "indexed_block",
                "Latest block that has been indexed for a contract in a specific chain.",
            ),
            &["chain_id", "contract_address"],
        )?;
        registry.register(Box::new(indexed_block.clone()))?;

        let chain_head_block = IntGaugeVec::new(
            Opts::new(
                "chain_head_block",
                "Latest block reported by the RPC node for a specific chain. Reported by contract.",
            ),
            &["chain_id", "contract_address"],
        )?;
        registry.register(Box::new(chain_head_block.clone()))?;

        // Standard build info style metric: value is always 1.
        let build_info = IntGaugeVec::new(
            Opts::new("build_info", "Build information about the running binary."),
            &["version"],
        )?;
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        registry.register(Box::new(build_info.clone()))?;

        Ok(Self {
            inner: Some(Arc::new(MetricsInner {
                registry,
                indexed_block,
                chain_head_block,
                allow_origin: config.allow_origin.clone(),
            })),
        })
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    #[inline]
    pub fn record_indexed_block(&self, chain_id: u64, contract: &str, block: u64) {
        if let Some(inner) = &self.inner {
            let chain_id_str = chain_id.to_string();
            inner
                .indexed_block
                .with_label_values(&[chain_id_str.as_str(), contract])
                .set(block as i64);
        }
    }

    #[inline]
    pub fn record_chain_head_block(&self, chain_id: u64, contract: &str, block: u64) {
        if let Some(inner) = &self.inner {
            let chain_id_str = chain_id.to_string();
            inner
                .chain_head_block
                .with_label_values(&[chain_id_str.as_str(), contract])
                .set(block as i64);
        }
    }

    pub async fn serve(&self, config: MetricsConfig) -> Result<Option<JoinHandle<()>>> {
        let Some(inner) = self.inner.clone() else {
            return Ok(None);
        };

        let addr: SocketAddr = format!("{}:{}", config.address, config.port).parse()?;
        let state = MetricsState {
            registry: inner.registry.clone(),
            allow_origin: inner.allow_origin.clone(),
        };

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(state);

        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .unwrap_or_else(|_| panic!("Failed to bind metrics server to {addr}"));
            tracing::info!(
                "Metrics server listening on {}",
                listener.local_addr().unwrap()
            );

            axum::serve(listener, app)
                .await
                .unwrap_or_else(|e| panic!("Metrics server error: {e}"));
        });

        Ok(Some(handle))
    }
}

#[derive(Clone)]
struct MetricsState {
    registry: Registry,
    allow_origin: Option<String>,
}

async fn metrics_handler(State(state): State<MetricsState>) -> Response {
    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();
    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!("Failed to encode metrics: {e}");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("failed to encode metrics"))
            .expect("response building should not fail");
    }

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .expect("response building should not fail");

    if let Some(origin) = state.allow_origin.as_ref() {
        let header_value =
            HeaderValue::from_str(origin).unwrap_or_else(|_| HeaderValue::from_static("*"));
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, header_value);
    }

    response
}
