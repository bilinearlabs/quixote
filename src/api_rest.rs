// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    CancellationToken,
    storage::{ContractDescriptorDb, DuckDBStorageFactory, EventDescriptorDb, Storage},
};
use anyhow::Result;
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, trace, warn};

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// Data type that represents the JSON response for the /list_events endpoint.
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

/// GET handler for /list_events
///
/// # Description
///
/// This handler is used to list all the events indexed in the database along their indexing status.
#[instrument(skip(factory))]
async fn list_events_handler(
    State(factory): State<Arc<DuckDBStorageFactory>>,
) -> Result<Json<ListEventsResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Create a new storage instance with a new connection for this request
    let storage = factory.create().map_err(|e| {
        error!("Failed to create database connection: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "An internal error occurred, try again later.".to_owned(),
            }),
        )
    })?;
    match storage.list_indexed_events() {
        Ok(events) => {
            debug!("Events listed successfully");
            trace!("Events: {:?}", events);
            Ok(Json(ListEventsResponse { events }))
        }
        Err(e) => {
            error!("Failed to list events: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "An internal error occurred, try again later.".to_owned(),
                }),
            ))
        }
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

/// POST handler for /raw_query.
///
/// # Description
///
/// This handler is used to execute SQL queries against the indexer database. Only SELECT queries are supported.
#[instrument(skip(factory, payload), fields(query = %payload.query))]
async fn raw_query_handler(
    State(factory): State<Arc<DuckDBStorageFactory>>,
    Json(payload): Json<RawQueryRequest>,
) -> Result<Json<RawQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    // First, check if the query is a SELECT query.
    if !payload
        .query
        .trim_start()
        .to_uppercase()
        .starts_with("SELECT")
    {
        warn!("Received a non-SELECT query: {}", payload.query);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Only SELECT queries are supported.".to_owned(),
            }),
        ));
    }

    // Create a new storage instance with a new connection for this request.
    let storage = factory.create().map_err(|e| {
        error!("Failed to create a database connection: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "An internal error occurred, try again later.".to_owned(),
            }),
        )
    })?;

    // Send the query to the handler.
    match storage.send_raw_query(&payload.query) {
        Ok(result) => {
            debug!("Query successfully served");
            trace!("Query result: {:?}", result);
            Ok(Json(RawQueryResponse {
                query_result: result,
            }))
        }
        Err(e) => {
            error!("Failed to serve query: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to execute the given SQL query.".to_owned(),
                }),
            ))
        }
    }
}

/// Creates and returns the REST API router
pub fn create_router(factory: Arc<DuckDBStorageFactory>) -> Router {
    Router::new()
        .route("/list_events", get(list_events_handler))
        .route("/list_contracts", get(list_contracts_handler))
        .route("/raw_query", post(raw_query_handler))
        .with_state(factory)
}

/// Starts the REST API server in a separate task
pub async fn start_api_server(
    server_address: &str,
    storage_backend: Arc<DuckDBStorageFactory>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let server_address = server_address.to_string();
    tokio::spawn(async move {
        let app = create_router(storage_backend);
        let port = server_address.split(":").nth(1).unwrap().to_string();

        let listener = tokio::net::TcpListener::bind(server_address)
            .await
            .unwrap_or_else(|_| panic!("Failed to bind to port {port}"));
        info!("API server listening on {}", listener.local_addr().unwrap());
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(cancellation_token))
            .await
            .expect("API server error");
    });

    Ok(())
}

async fn shutdown_signal(cancellation_token: CancellationToken) {
    let _ = cancellation_token.subscribe().recv().await;
    tracing::warn!("API server shutdown signal received");
}
