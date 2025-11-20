// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    CancellationToken,
    storage::{
        ContractDescriptorDb, DuckDBStorageFactory, EventDb, EventDescriptorDb, StorageQuery,
    },
};
use alloy::primitives::Address;
use anyhow::Result;
use axum::{Router, extract::State, http::StatusCode, response::Json, routing::post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// Request/Response types for list_events endpoint
#[derive(Serialize)]
pub struct ListEventsResponse {
    pub events: Vec<EventDescriptorDb>,
}

// Request/Response types for list_contracts endpoint
#[derive(Serialize)]
pub struct ListContractsResponse {
    pub contracts: Vec<ContractDescriptorDb>,
}

// Request type for get_events endpoint
#[derive(Deserialize)]
pub struct GetEventsRequest {
    #[serde(default)]
    #[allow(dead_code)] // Not used in implementation, but kept for API compatibility
    pub event: Option<String>, // Not used in implementation, but required by trait
    pub contract: String,
    pub start_time: String,       // ISO 8601 format (RFC3339)
    pub end_time: Option<String>, // ISO 8601 format (RFC3339)
}

// Response type for get_events endpoint
#[derive(Serialize)]
pub struct GetEventsResponse {
    pub events: Vec<EventDb>,
}

// Request type for raw_query endpoint
#[derive(Deserialize)]
pub struct RawQueryRequest {
    pub query: String,
}

// Response type for raw_query endpoint
#[derive(Serialize)]
pub struct RawQueryResponse {
    pub query_result: Value,
}

// POST handler for list_events
async fn list_events_handler(
    State(factory): State<Arc<DuckDBStorageFactory>>,
) -> Result<Json<ListEventsResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Create a new storage instance with a new connection for this request
    let storage = factory.create().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to create database connection: {}", e),
            }),
        )
    })?;
    match storage.list_events() {
        Ok(events) => Ok(Json(ListEventsResponse { events })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

// POST handler for list_contracts
async fn list_contracts_handler(
    State(factory): State<Arc<DuckDBStorageFactory>>,
) -> Result<Json<ListContractsResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Create a new storage instance with a new connection for this request
    let storage = factory.create().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to create database connection: {}", e),
            }),
        )
    })?;
    match storage.list_contracts() {
        Ok(contracts) => Ok(Json(ListContractsResponse { contracts })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

// POST handler for get_events
async fn get_events_handler(
    State(factory): State<Arc<DuckDBStorageFactory>>,
    Json(payload): Json<GetEventsRequest>,
) -> Result<Json<GetEventsResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Create a new storage instance with a new connection for this request
    let storage = factory.create().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to create database connection: {}", e),
            }),
        )
    })?;
    // Parse contract address
    let contract = payload.contract.parse::<Address>().map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Invalid contract address: {}", e),
            }),
        )
    })?;

    // Parse start_time
    let start_time = DateTime::parse_from_rfc3339(&payload.start_time)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid start_time format (expected RFC3339): {}", e),
                }),
            )
        })?
        .with_timezone(&Utc);

    // Parse end_time if provided
    let end_time = if let Some(end_time_str) = payload.end_time {
        Some(
            DateTime::parse_from_rfc3339(&end_time_str)
                .map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("Invalid end_time format (expected RFC3339): {}", e),
                        }),
                    )
                })?
                .with_timezone(&Utc),
        )
    } else {
        None
    };

    // Create a dummy Event since it's not used in the implementation
    // Using a placeholder ERC20 Transfer event
    let dummy_event = alloy::json_abi::Event::parse(
        "Transfer(address indexed from, address indexed to, uint256 amount)",
    )
    .unwrap();

    match storage.get_events(dummy_event, contract, start_time, end_time) {
        Ok(events) => Ok(Json(GetEventsResponse { events })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

// POST handler for raw_query
async fn raw_query_handler(
    State(factory): State<Arc<DuckDBStorageFactory>>,
    Json(payload): Json<RawQueryRequest>,
) -> Result<Json<RawQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Create a new storage instance with a new connection for this request
    let storage = factory.create().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to create database connection: {}", e),
            }),
        )
    })?;
    match storage.send_raw_query(&payload.query) {
        Ok(result) => Ok(Json(RawQueryResponse {
            query_result: result,
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

/// Creates and returns the REST API router
pub fn create_router(factory: Arc<DuckDBStorageFactory>) -> Router {
    Router::new()
        .route("/list_events", post(list_events_handler))
        .route("/list_contracts", post(list_contracts_handler))
        .route("/get_events", post(get_events_handler))
        .route("/raw_query", post(raw_query_handler))
        .with_state(factory)
}

/// Starts the REST API server in a separate thread
pub fn start_api_server(
    server_address: &str,
    storage_backend: Arc<DuckDBStorageFactory>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let server_address = server_address.to_string();
    let _ = tokio::spawn(async move {
        let app = create_router(storage_backend);
        let port = server_address.split(":").nth(1).unwrap().to_string();

        println!("REST API server listening on {server_address}");
        let listener = tokio::net::TcpListener::bind(server_address)
            .await
            .expect(&format!("Failed to bind to port {port}"));
        axum::serve(listener, app)
            .with_graceful_shutdown(sthutdown_signal(cancellation_token))
            .await
            .expect("API server error");
    });

    Ok(())
}

async fn sthutdown_signal(cancellation_token: CancellationToken) {
    let _ = cancellation_token.subscribe().recv().await;
    println!("API server shutdown signal received");
}
