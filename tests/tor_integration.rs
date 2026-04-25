// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT
//
//! Integration tests for the Tor hidden-service layer.
//!
//! These tests cover:
//! 1. `/tor-info` returns `enabled: false` when Tor is not active.
//! 2. `/tor-info` returns `enabled: true` and a valid onion address
//!    when the binary is compiled with `--features tor` and Tor is active.
//! 3. CORS headers are added for `.onion` origins but not for regular ones.
//! 4. Privacy middleware strips `User-Agent`, `Referer`, and `X-Forwarded-For`.
//! 5. `/tor-info` includes `circuits_active` and `circuits_rejected` counters.

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use quixote::api_rest::create_router;
use std::sync::Arc;
use tower::ServiceExt; // for Router::oneshot

// ── helpers ───────────────────────────────────────────────────────────────────

/// A minimal in-memory storage factory that satisfies the type constraint
/// without needing a real database.
mod stub_storage {
    use alloy::json_abi::Event;
    use alloy::{primitives::B256, rpc::types::Log};
    use anyhow::Result;
    use async_trait::async_trait;
    use quixote::{
        EventStatus,
        storage::{ContractDescriptorDb, EventDescriptorDb, Storage, StorageFactory},
    };
    use serde_json::Value;

    #[derive(Default)]
    pub struct Stub;

    #[async_trait]
    impl Storage for Stub {
        async fn add_events(&self, _: u64, _: &[Log]) -> Result<()> {
            Ok(())
        }
        async fn list_indexed_events(&self) -> Result<Vec<EventDescriptorDb>> {
            Ok(vec![])
        }
        async fn event_index_status(&self, _: u64, _: &Event) -> Result<Option<EventStatus>> {
            Ok(None)
        }
        async fn include_events(&self, _: u64, _: &[Event]) -> Result<()> {
            Ok(())
        }
        async fn get_event_signature(&self, _: &str) -> Result<String> {
            Ok(String::new())
        }
        async fn last_block(&self, _: u64, _: &Event) -> Result<u64> {
            Ok(0)
        }
        async fn first_block(&self, _: u64, _: &Event) -> Result<u64> {
            Ok(0)
        }
        async fn set_first_block(&self, _: u64, _: &Event, _: u64) -> Result<()> {
            Ok(())
        }
        async fn synchronize_events(&self, _: u64, _: &[B256], _: Option<u64>) -> Result<()> {
            Ok(())
        }
        async fn send_raw_query(&self, _: &str) -> Result<Value> {
            Ok(Value::Null)
        }
        async fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>> {
            Ok(vec![])
        }
        async fn describe_database(&self) -> Result<Value> {
            Ok(Value::Null)
        }
    }

    impl StorageFactory for Stub {
        fn create_storage(&self) -> Result<Box<dyn Storage>> {
            Ok(Box::new(Stub))
        }
    }
}

fn make_router(tor_state: quixote::api_rest::TorStateHandle) -> axum::Router {
    let factory: Arc<dyn quixote::storage::StorageFactory> = Arc::new(stub_storage::Stub);
    create_router(factory, tor_state)
}

// ── /tor-info without Tor ─────────────────────────────────────────────────────

#[tokio::test]
async fn tor_info_returns_disabled_when_tor_not_active() {
    let router = make_router(None);

    let req = Request::builder()
        .uri("/tor-info")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["enabled"], false);
    assert_eq!(json["bootstrapped"], false);
    assert_eq!(json["streams_accepted"], 0);
    assert_eq!(json["circuits_active"], 0);
    assert_eq!(json["circuits_rejected"], 0);
}

// ── /tor-info with Tor feature compiled in ───────────────────────────────────

#[cfg(feature = "tor")]
#[tokio::test]
async fn tor_info_returns_enabled_with_tor_state() {
    use quixote::tor_service::TorState;

    // Create a TorState without connecting to the Tor network —
    // we only need the shared handle to verify /tor-info returns enabled: true.
    let state = TorState::new();
    let router = make_router(Some(state));

    let req = Request::builder()
        .uri("/tor-info")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["enabled"], true);
}

// ── CORS middleware ───────────────────────────────────────────────────────────

#[tokio::test]
async fn cors_header_added_for_onion_origin() {
    let router = make_router(None);

    let onion_origin = "http://exampleonionaddress56chars1234567890abcdef.onion";
    let req = Request::builder()
        .uri("/tor-info")
        .header(header::ORIGIN, onion_origin)
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();

    let acao = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .and_then(|v| v.to_str().ok());

    assert_eq!(
        acao,
        Some(onion_origin),
        "ACAO header must echo the .onion origin"
    );
}

#[tokio::test]
async fn cors_header_not_added_for_regular_origin() {
    let router = make_router(None);

    let req = Request::builder()
        .uri("/tor-info")
        .header(header::ORIGIN, "https://example.com")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();

    let acao = response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN);

    assert!(
        acao.is_none(),
        "ACAO header must NOT be set for non-.onion origins"
    );
}

// ── Privacy-conscious logging / header stripping ──────────────────────────────

#[tokio::test]
async fn privacy_middleware_strips_identifying_headers() {
    use axum::{Router, middleware, routing::get};
    use quixote::api_rest::strip_identifying_headers;

    // Minimal router that echoes whether each sensitive header is present.
    let test_router = Router::new()
        .route(
            "/check",
            get(|req: axum::extract::Request| async move {
                let h = req.headers();
                let has_ua = h.contains_key(header::USER_AGENT);
                let has_ref = h.contains_key(header::REFERER);
                let has_xff = h.contains_key("x-forwarded-for");
                format!("{has_ua},{has_ref},{has_xff}")
            }),
        )
        .layer(middleware::from_fn(strip_identifying_headers));

    let req = Request::builder()
        .uri("/check")
        .header(header::USER_AGENT, "Mozilla/5.0 TorBrowser/14.0")
        .header(header::REFERER, "https://example.com/page")
        .header("x-forwarded-for", "203.0.113.1")
        .body(Body::empty())
        .unwrap();

    let response = test_router.oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(
        &body[..],
        b"false,false,false",
        "User-Agent, Referer, and X-Forwarded-For must be stripped before the handler sees them"
    );
}

// ── Circuit stats in /tor-info ────────────────────────────────────────────────

#[cfg(feature = "tor")]
#[tokio::test]
async fn tor_info_includes_circuit_stats() {
    use quixote::tor_service::TorState;

    let state = TorState::new();
    let router = make_router(Some(state));

    let req = Request::builder()
        .uri("/tor-info")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["enabled"], true);
    assert_eq!(json["circuits_active"], 0);
    assert_eq!(json["circuits_rejected"], 0);
}
