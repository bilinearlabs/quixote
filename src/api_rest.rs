// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

use crate::{
    CancellationToken,
    storage::{ContractDescriptorDb, EventDescriptorDb, StorageFactory},
};
use anyhow::Result;
use axum::{
    Router,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Json, Response},
    routing::{get, post},
};
use axum::http::Request;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, trace, warn};

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Data type that represents the JSON response for the /list_events endpoint.
#[derive(Serialize)]
pub struct ListEventsResponse {
    pub events: Vec<EventDescriptorDb>,
}

/// Request/Response types for list_contracts endpoint
#[derive(Serialize)]
pub struct ListContractsResponse {
    pub contracts: Vec<ContractDescriptorDb>,
}

/// Request type for raw_query endpoint
#[derive(Deserialize)]
pub struct RawQueryRequest {
    pub query: String,
}

/// Response type for raw_query endpoint
#[derive(Serialize)]
pub struct RawQueryResponse {
    pub query_result: Value,
}

/// Response type for db_schema endpoint
#[derive(Serialize)]
pub struct DbSchemaResponse {
    pub schema: Value,
}

/// Response type for the /tor-info endpoint.
#[derive(Serialize)]
pub struct TorInfoResponse {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onion_address: Option<String>,
    pub bootstrapped: bool,
    pub streams_accepted: u64,
    /// Currently active circuits (rendezvous connections).
    pub circuits_active: u64,
    /// Total circuits rejected by the rate limiter since startup.
    pub circuits_rejected: u64,
}

/// Shared Tor state injected into the router when `--tor` is active.
///
/// When Tor is disabled this is `None` and `/tor-info` returns `enabled: false`.
#[cfg(feature = "tor")]
pub type TorStateHandle = Option<crate::tor_service::TorState>;
#[cfg(not(feature = "tor"))]
pub type TorStateHandle = Option<()>;

/// GET handler for /list_events
///
/// # Description
///
/// This handler is used to list all the events indexed in the database along their indexing status.
#[instrument(skip(state))]
async fn list_events_handler(
    State(state): State<AppState>,
) -> Result<Json<ListEventsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let factory = &state.0;
    // Create a new storage instance with a new connection for this request
    let storage = factory.create_storage().map_err(|e| {
        error!("Failed to create database connection: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "An internal error occurred, try again later.".to_owned(),
            }),
        )
    })?;
    match storage.list_indexed_events().await {
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

/// GET handler for /list_contracts
///
/// # Description
///
/// This handler is used to list all the contracts indexed in the database.
#[instrument(skip(state))]
async fn list_contracts_handler(
    State(state): State<AppState>,
) -> Result<Json<ListContractsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let factory = &state.0;
    // Create a new storage instance with a new connection for this request
    let storage = factory.create_storage().map_err(|e| {
        error!("Failed to create database connection: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "An internal error occurred, try again later.".to_owned(),
            }),
        )
    })?;
    match storage.list_contracts().await {
        Ok(contracts) => {
            debug!("Contracts listed successfully");
            trace!("Contracts: {:?}", contracts);
            Ok(Json(ListContractsResponse { contracts }))
        }
        Err(e) => {
            error!("Failed to list contracts: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "An internal error occurred, try again later.".to_owned(),
                }),
            ))
        }
    }
}

/// POST handler for /raw_query.
///
/// # Description
///
/// This handler is used to execute SQL queries against the indexer database. Only SELECT queries are supported.
#[instrument(skip(state, payload), fields(query = %payload.query))]
async fn raw_query_handler(
    State(state): State<AppState>,
    Json(payload): Json<RawQueryRequest>,
) -> Result<Json<RawQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let factory = &state.0;
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
    let storage = factory.create_storage().map_err(|e| {
        error!("Failed to create a database connection: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "An internal error occurred, try again later.".to_owned(),
            }),
        )
    })?;

    // Send the query to the handler.
    match storage.send_raw_query(&payload.query).await {
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

/// GET handler for /db_schema
///
/// # Description
///
/// This handler returns the database schema including all tables (except quixote_info)
/// and their column definitions.
#[instrument(skip(state))]
async fn db_schema_handler(
    State(state): State<AppState>,
) -> Result<Json<DbSchemaResponse>, (StatusCode, Json<ErrorResponse>)> {
    let factory = &state.0;
    let storage = factory.create_storage().map_err(|e| {
        error!("Failed to create database connection: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "An internal error occurred, try again later.".to_owned(),
            }),
        )
    })?;

    match storage.describe_database().await {
        Ok(schema) => {
            debug!("Database schema retrieved successfully");
            trace!("Schema: {:?}", schema);
            Ok(Json(DbSchemaResponse { schema }))
        }
        Err(e) => {
            error!("Failed to retrieve database schema: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "An internal error occurred, try again later.".to_owned(),
                }),
            ))
        }
    }
}

/// GET handler for /tor-info
///
/// Returns metadata about the Tor hidden service: onion address, bootstrap status,
/// and number of circuits (streams) accepted so far.  Always present regardless of
/// whether Tor is enabled so callers can discover it without prior knowledge.
async fn tor_info_handler(
    State((_, tor_state)): State<(Arc<dyn StorageFactory>, TorStateHandle)>,
) -> Json<TorInfoResponse> {
    #[cfg(feature = "tor")]
    if let Some(state) = &tor_state {
        let info = state.info().await;
        return Json(TorInfoResponse {
            enabled: true,
            onion_address: info.onion_address,
            bootstrapped: info.bootstrapped,
            streams_accepted: info.streams_accepted,
            circuits_active: info.circuits_active,
            circuits_rejected: info.circuits_rejected,
        });
    }

    // Tor not enabled (either binary lacks the feature or --tor was not passed).
    let _ = tor_state; // suppress unused-variable warning in non-tor builds
    Json(TorInfoResponse {
        enabled: false,
        onion_address: None,
        bootstrapped: false,
        streams_accepted: 0,
        circuits_active: 0,
        circuits_rejected: 0,
    })
}

/// Axum middleware that removes Tor-sensitive fingerprinting headers.
///
/// Applied to the router served over Tor so that clients cannot inadvertently
/// leak browser fingerprint data (`User-Agent`, `Referer`) or network topology
/// (`X-Forwarded-For`) through their request headers.
pub async fn strip_identifying_headers(
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let headers = req.headers_mut();
    headers.remove(header::USER_AGENT);
    headers.remove(header::REFERER);
    headers.remove("x-forwarded-for");
    next.run(req).await
}

/// Axum middleware that adds `Access-Control-Allow-Origin` for `.onion` origins.
///
/// This allows browser clients running inside Tor Browser (whose origin is a
/// `.onion` URL) to call the API without CORS errors.
async fn onion_cors_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let mut response = next.run(req).await;

    if let Some(origin) = origin {
        if origin.ends_with(".onion") || origin.ends_with(".onion/") {
            if let Ok(val) = HeaderValue::from_str(&origin) {
                response.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, val);
            }
        }
    }

    response
}

/// Combined state passed to every handler: storage factory + optional Tor state.
type AppState = (Arc<dyn StorageFactory>, TorStateHandle);

/// Creates and returns the REST API router
pub fn create_router(factory: Arc<dyn StorageFactory>, tor_state: TorStateHandle) -> Router {
    let state: AppState = (factory, tor_state);
    Router::new()
        .route("/list_events", get(list_events_handler))
        .route("/list_contracts", get(list_contracts_handler))
        .route("/raw_query", post(raw_query_handler))
        .route("/db_schema", get(db_schema_handler))
        .route("/tor-info", get(tor_info_handler))
        .layer(middleware::from_fn(onion_cors_middleware))
        .with_state(state)
}

/// Starts the REST API server in a separate task
pub async fn start_api_server(
    server_address: &str,
    storage_backend: Arc<dyn StorageFactory>,
    tor_state: TorStateHandle,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let server_address = server_address.to_string();
    tokio::spawn(async move {
        let app = create_router(storage_backend, tor_state);
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
