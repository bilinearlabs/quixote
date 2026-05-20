// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

use crate::configuration::GraphqlLayerConfig;
use crate::storage::{
    EventColumn, EventDescriptorDb, EventQuery, FilterOp, FilterValue, OrderDir, StorageFactory,
    WhereClause,
};
use anyhow::Result;
use async_graphql::{
    Value as GqlValue,
    dynamic::{Field, FieldFuture, FieldValue, InputObject, InputValue, Object, Schema, TypeRef},
    http::playground_source,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use std::sync::Arc;

const DEFAULT_FIRST: u64 = 100;
const MAX_FIRST: u64 = 10_000;

/// Map a GraphQL camelCase field name to its snake_case database column name.
fn gql_col_to_db(gql: &str) -> String {
    match gql {
        "blockNumber" => "block_number".to_string(),
        "transactionHash" => "transaction_hash".to_string(),
        "logIndex" => "log_index".to_string(),
        "contractAddress" => "contract_address".to_string(),
        other => other.to_string(),
    }
}

/// Build a dynamic GraphQL schema from the event descriptors currently in the database.
///
/// Each indexed event type becomes a GraphQL object type and a top-level query field.
/// Resolvers call `query_event_table` so callers get live data.
pub fn build_schema(
    events: Vec<EventDescriptorDb>,
    factory: Arc<dyn StorageFactory>,
    graphql_config: Option<&GraphqlLayerConfig>,
) -> Result<Schema> {
    // _empty is a fallback field required by the spec when no events are indexed yet.
    let mut query = if events.is_empty() {
        Object::new("Query").field(Field::new(
            "_empty",
            TypeRef::named(TypeRef::BOOLEAN),
            |_| FieldFuture::from_value(None),
        ))
    } else {
        Object::new("Query")
    };

    let mut schema_builder = Schema::build("Query", None, None);

    // Shared input type: one field per filter operator.
    let string_filter = InputObject::new("StringFilter")
        .field(InputValue::new("eq", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("in", TypeRef::named_list(TypeRef::STRING)))
        .field(InputValue::new("gt", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("lt", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("gte", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new("lte", TypeRef::named(TypeRef::STRING)));
    schema_builder = schema_builder.register(string_filter);

    for event in events {
        let (Some(event_name), Some(chain_id), Some(event_hash), Some(sig)) = (
            event.event_name.as_ref(),
            event.chain_id,
            event.event_hash.as_ref(),
            event.event_signature.as_ref(),
        ) else {
            continue;
        };

        let columns = parse_event_columns(sig);

        // Reproduce the table-name logic from the storage backends.
        let start = if event_hash.starts_with("0x") { 2 } else { 0 };
        let short_hash = &event_hash[start..(start + 5).min(event_hash.len())];
        let table_name = format!(
            "event_{}_{}_{}",
            chain_id,
            event_name.to_ascii_lowercase(),
            short_hash
        );

        // GraphQL type and field name mirror the SQL table name.
        let type_name = table_name.clone();
        let field_name = table_name.clone();

        // --- Build the per-event WHERE input type (field-level operators) ---
        let where_type_name = format!("{type_name}_where");
        let mut where_input = InputObject::new(&where_type_name);
        for &gql_name in &[
            "blockNumber",
            "transactionHash",
            "logIndex",
            "contractAddress",
        ] {
            where_input =
                where_input.field(InputValue::new(gql_name, TypeRef::named("StringFilter")));
        }
        for col in &columns {
            where_input =
                where_input.field(InputValue::new(&col.name, TypeRef::named("StringFilter")));
        }
        schema_builder = schema_builder.register(where_input);

        // --- Build the per-event CONDITION input type (exact-match shorthand) ---
        let condition_type_name = format!("{type_name}_condition");
        let mut condition_input = InputObject::new(&condition_type_name);
        for &gql_name in &[
            "blockNumber",
            "transactionHash",
            "logIndex",
            "contractAddress",
        ] {
            condition_input =
                condition_input.field(InputValue::new(gql_name, TypeRef::named(TypeRef::STRING)));
        }
        for col in &columns {
            condition_input =
                condition_input.field(InputValue::new(&col.name, TypeRef::named(TypeRef::STRING)));
        }
        schema_builder = schema_builder.register(condition_input);

        // --- Build the object type for this event ---
        let mut obj = Object::new(&type_name);
        obj = add_fixed_fields(obj);
        for col in &columns {
            let col_name = col.name.clone();
            obj = obj.field(Field::new(
                col_name.clone(),
                TypeRef::named(TypeRef::STRING),
                move |ctx| {
                    let col_name = col_name.clone();
                    FieldFuture::new(async move {
                        let val = ctx
                            .parent_value
                            .downcast_ref::<serde_json::Value>()
                            .and_then(|v| v.get(&col_name))
                            .map(json_to_gql);
                        Ok(val.map(FieldValue::value))
                    })
                },
            ));
        }
        schema_builder = schema_builder.register(obj);

        // --- Build the query field with resolver ---
        let factory_c = factory.clone();
        let table_name_c = table_name.clone();
        let columns_c = columns.clone();
        let type_name_c = type_name.clone();
        let where_type_name_c = where_type_name.clone();
        let condition_type_name_c = condition_type_name.clone();

        query = query.field(
            Field::new(
                field_name,
                TypeRef::named_nn_list_nn(&type_name_c),
                move |ctx| {
                    let factory = factory_c.clone();
                    let table_name = table_name_c.clone();
                    let columns = columns_c.clone();

                    FieldFuture::new(async move {
                        let storage = factory
                            .create_storage()
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        // --- Parse `where` ---
                        let mut where_clauses: Vec<WhereClause> = Vec::new();
                        if let Some(where_val) = ctx.args.get("where") {
                            let where_obj = where_val.object().map_err(|e| {
                                async_graphql::Error::new(format!("invalid 'where': {e:?}"))
                            })?;
                            for (gql_col, filter_val) in where_obj.iter() {
                                let gql_col: &str = gql_col.as_ref();
                                let db_col = gql_col_to_db(gql_col);
                                let filter_obj = filter_val.object().map_err(|e| {
                                    async_graphql::Error::new(format!("invalid filter for '{gql_col}': {e:?}"))
                                })?;
                                for (op_str, op_val) in filter_obj.iter() {
                                    let op = match op_str.as_ref() {
                                        "eq"  => FilterOp::Eq,
                                        "in"  => FilterOp::In,
                                        "gt"  => FilterOp::Gt,
                                        "lt"  => FilterOp::Lt,
                                        "gte" => FilterOp::Gte,
                                        "lte" => FilterOp::Lte,
                                        other => return Err(async_graphql::Error::new(
                                            format!("unknown operator '{other}' — use eq, in, gt, lt, gte, lte"),
                                        )),
                                    };
                                    let value = if matches!(op, FilterOp::In) {
                                        let list = op_val.list().map_err(|_| {
                                            async_graphql::Error::new("'in' requires a list value")
                                        })?;
                                        let items: Vec<String> = list
                                            .iter()
                                            .map(|v| {
                                                v.string().map(str::to_string).map_err(|_| {
                                                    async_graphql::Error::new(
                                                        "'in' list items must be strings",
                                                    )
                                                })
                                            })
                                            .collect::<async_graphql::Result<_>>()?;
                                        if items.is_empty() {
                                            return Err(async_graphql::Error::new(
                                                "'in' list must not be empty",
                                            ));
                                        }
                                        FilterValue::List(items)
                                    } else {
                                        FilterValue::Scalar(
                                            op_val
                                                .string()
                                                .map_err(|_| {
                                                    async_graphql::Error::new(
                                                        "filter value must be a string",
                                                    )
                                                })?
                                                .to_string(),
                                        )
                                    };
                                    where_clauses.push(WhereClause {
                                        column: db_col.clone(),
                                        op,
                                        value,
                                    });
                                }
                            }
                        }

                        // --- Parse `condition` (exact-match shorthand) ---
                        if let Some(cond_val) = ctx.args.get("condition") {
                            let cond_obj = cond_val.object().map_err(|e| {
                                async_graphql::Error::new(format!("invalid 'condition': {e:?}"))
                            })?;
                            for (gql_col, val) in cond_obj.iter() {
                                let gql_col: &str = gql_col.as_ref();
                                let db_col = gql_col_to_db(gql_col);
                                let s = val.string().map_err(|_| {
                                    async_graphql::Error::new(format!(
                                        "condition value for '{gql_col}' must be a string"
                                    ))
                                })?;
                                where_clauses.push(WhereClause {
                                    column: db_col,
                                    op: FilterOp::Eq,
                                    value: FilterValue::Scalar(s.to_string()),
                                });
                            }
                        }

                        // --- Parse `orderBy` ---
                        let order_by = if let Some(v) = ctx.args.get("orderBy") {
                            let gql_name = v.string().map_err(|_| {
                                async_graphql::Error::new("orderBy must be a string")
                            })?;
                            let db_col = gql_col_to_db(gql_name);
                            let is_fixed = matches!(
                                db_col.as_str(),
                                "block_number" | "transaction_hash" | "log_index" | "contract_address"
                            );
                            let is_param = columns.iter().any(|c| c.name == db_col);
                            if !is_fixed && !is_param {
                                return Err(async_graphql::Error::new(format!(
                                    "unknown orderBy column '{gql_name}'"
                                )));
                            }
                            Some(db_col)
                        } else {
                            None
                        };

                        // --- Parse `orderDir` ---
                        let order_dir = if let Some(v) = ctx.args.get("orderDir") {
                            let s = v.string().map_err(|_| {
                                async_graphql::Error::new("orderDir must be a string")
                            })?;
                            match s.to_uppercase().as_str() {
                                "ASC"  => OrderDir::Asc,
                                "DESC" => OrderDir::Desc,
                                other  => return Err(async_graphql::Error::new(format!(
                                    "orderDir must be ASC or DESC, got '{other}'"
                                ))),
                            }
                        } else {
                            OrderDir::default()
                        };

                        // --- Parse `first` / `skip` ---
                        let first = ctx
                            .args
                            .get("first")
                            .and_then(|v| v.i64().ok())
                            .map(|n| (n as u64).min(MAX_FIRST))
                            .unwrap_or(DEFAULT_FIRST);
                        let skip = ctx
                            .args
                            .get("skip")
                            .and_then(|v| v.i64().ok())
                            .map(|n| n as u64)
                            .unwrap_or(0);

                        let rows = storage
                            .query_event_table(&EventQuery {
                                table_name,
                                columns,
                                where_clauses,
                                order_by,
                                order_dir,
                                first,
                                skip,
                            })
                            .await
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        Ok(Some(FieldValue::list(
                            rows.into_iter().map(FieldValue::owned_any),
                        )))
                    })
                },
            )
            .argument(InputValue::new("where",     TypeRef::named(&where_type_name_c)))
            .argument(InputValue::new("condition", TypeRef::named(&condition_type_name_c)))
            .argument(InputValue::new("orderBy",   TypeRef::named(TypeRef::STRING)))
            .argument(InputValue::new("orderDir",  TypeRef::named(TypeRef::STRING)))
            .argument(InputValue::new("first",     TypeRef::named(TypeRef::INT)))
            .argument(InputValue::new("skip",      TypeRef::named(TypeRef::INT))),
        );
    }

    if let Some(cfg) = graphql_config {
        (schema_builder, query) =
            crate::api_the_graph::register_the_graph_entities(schema_builder, query, cfg, factory)
                .map_err(|e| anyhow::anyhow!("Failed to register The Graph entities: {e}"))?;
    }

    schema_builder
        .register(query)
        .finish()
        .map_err(|e| anyhow::anyhow!("Failed to build GraphQL schema: {e}"))
}

/// Attach the four fixed event-table columns (block_number, transaction_hash, log_index,
/// contract_address) to an object type, mapping snake_case DB names to camelCase GQL names.
fn add_fixed_fields(mut obj: Object) -> Object {
    let fixed: &[(&str, &str)] = &[
        ("blockNumber", "block_number"),
        ("transactionHash", "transaction_hash"),
        ("logIndex", "log_index"),
        ("contractAddress", "contract_address"),
    ];
    for &(gql_name, json_key) in fixed {
        obj = obj.field(Field::new(
            gql_name,
            TypeRef::named(TypeRef::STRING),
            move |ctx| {
                FieldFuture::new(async move {
                    let val = ctx
                        .parent_value
                        .downcast_ref::<serde_json::Value>()
                        .and_then(|v| v.get(json_key))
                        .map(json_to_gql);
                    Ok(val.map(FieldValue::value))
                })
            },
        ));
    }
    obj
}

/// Convert a serde_json value to its GraphQL scalar equivalent (everything stringified).
fn json_to_gql(v: &serde_json::Value) -> GqlValue {
    match v {
        serde_json::Value::String(s) => GqlValue::String(s.clone()),
        serde_json::Value::Number(n) => GqlValue::String(n.to_string()),
        serde_json::Value::Bool(b) => GqlValue::Boolean(*b),
        serde_json::Value::Null => GqlValue::Null,
        other => GqlValue::String(other.to_string()),
    }
}

/// Parse event parameter columns from a full ABI event signature.
/// Falls back to an empty list if the signature cannot be parsed.
fn parse_event_columns(sig: &str) -> Vec<EventColumn> {
    match alloy::json_abi::Event::parse(sig) {
        Ok(event) => event
            .inputs
            .iter()
            .map(|p| EventColumn {
                name: p.name.clone(),
                selector_type: p.selector_type().to_string(),
            })
            .collect(),
        Err(_) => vec![],
    }
}

// --- Axum glue ---

/// Strip variable declarations that are declared in an operation signature but never
/// referenced in the selection set.
///
/// The Graph's query engine ignores such "phantom" variables; the GraphQL spec (and
/// `async_graphql`) treat them as a validation error. This preprocessor removes them
/// so that queries written for The Graph work transparently on this server.
///
/// Algorithm: count how many times each `$name` token appears in the raw query string.
/// A variable that appears exactly once is only in its own declaration — it is never
/// used in the body. Those declarations are then removed from the operation's
/// variable-list.
fn strip_unused_variable_declarations(query: &str) -> String {
    use regex::Regex;
    use std::collections::{HashMap, HashSet};

    // Fast-path: no variables at all.
    if !query.contains('$') {
        return query.to_string();
    }

    // Count occurrences of every `$varName` token in the query.
    let token_re = Regex::new(r"\$(\w+)").expect("static regex");
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for cap in token_re.captures_iter(query) {
        if let Some(m) = cap.get(1) {
            *counts.entry(m.as_str()).or_insert(0) += 1;
        }
    }

    // Variables that appear only once are solely in their declaration, never in the body.
    let unused: HashSet<&str> = counts
        .iter()
        .filter_map(|(&name, &n)| if n == 1 { Some(name) } else { None })
        .collect();

    if unused.is_empty() {
        return query.to_string();
    }

    // Rewrite each operation's variable list, dropping unused declarations.
    // Matches: `query Name(` or `mutation Name(` etc. — the parens contain the var list.
    let op_re = Regex::new(r"(?s)((?:query|mutation|subscription)\s+\w*\s*)\(([^)]*)\)")
        .expect("static regex");

    op_re
        .replace_all(query, |caps: &regex::Captures| {
            let op_prefix = &caps[1];
            let var_list = &caps[2];

            let kept: Vec<&str> = var_list
                .split(',')
                .map(str::trim)
                .filter(|decl| {
                    // Each declaration looks like `$name: Type[!][= default]`.
                    // Keep it unless the name is in the unused set.
                    decl.find(':').is_none_or(|colon_idx| {
                        let name_part = decl[..colon_idx].trim();
                        name_part
                            .strip_prefix('$')
                            .map(|n| !unused.contains(n.trim()))
                            .unwrap_or(true)
                    })
                })
                .filter(|s| !s.is_empty())
                .collect();

            if kept.is_empty() {
                op_prefix.to_string()
            } else {
                format!("{}({})", op_prefix, kept.join(", "))
            }
        })
        .into_owned()
}

pub async fn graphql_handler(State(schema): State<Schema>, req: GraphQLRequest) -> GraphQLResponse {
    let mut request = req.into_inner();
    request.query = strip_unused_variable_declarations(&request.query);
    schema.execute(request).await.into()
}

pub async fn playground_handler() -> impl IntoResponse {
    Html(playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

/// Build the Axum router for the GraphQL endpoint.
///
/// When `playground` is `true`, the interactive GraphQL Playground UI is served
/// at `GET /graphql/playground`.
///
/// Note: introspection (`__schema`, `__type`) is enabled by default on the
/// `POST /graphql` endpoint regardless of this setting — it is a property of
/// the schema, not the playground.
pub fn create_graphql_router(schema: Schema, playground: bool) -> Router {
    let mut router = Router::new().route("/graphql", post(graphql_handler));
    if playground {
        router = router.route("/graphql/playground", get(playground_handler));
    }
    router.with_state(schema)
}

/// Build the schema from the factory (connects to DB, reads event descriptors).
pub async fn build_schema_from_factory(
    factory: Arc<dyn StorageFactory>,
    graphql_config: Option<&GraphqlLayerConfig>,
) -> Result<Schema> {
    let storage = factory.create_storage()?;
    let events = storage.list_indexed_events().await?;
    build_schema(events, factory, graphql_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DuckDBStorage, Storage};
    use crate::test_utils::*;
    use std::sync::Arc;

    // ── strip_unused_variable_declarations tests ──────────────────────────────

    #[test]
    fn strip_unused_vars_noop_when_all_used() {
        let q = r#"query getProposals($first: Int!, $skip: Int!) {
  proposals(first: $first, skip: $skip) { id }
}"#;
        assert_eq!(strip_unused_variable_declarations(q), q);
    }

    #[test]
    fn strip_unused_vars_removes_trailing_unused_var() {
        let q = r#"query getProposals($first: Int!, $skip: Int!, $stateFilter: Int) {
  proposals(first: $first, skip: $skip) { id }
}"#;
        let out = strip_unused_variable_declarations(q);
        assert!(
            !out.contains("stateFilter"),
            "stateFilter should be removed"
        );
        assert!(out.contains("$first"), "$first should remain");
        assert!(out.contains("$skip"), "$skip should remain");
    }

    #[test]
    fn strip_unused_vars_removes_leading_unused_var() {
        let q = r#"query Q($unused: String, $id: ID!) { node(id: $id) { name } }"#;
        let out = strip_unused_variable_declarations(q);
        assert!(!out.contains("$unused"), "unused should be removed");
        assert!(out.contains("$id"), "id should remain");
    }

    #[test]
    fn strip_unused_vars_removes_all_vars_when_none_used() {
        let q = r#"query Q($a: String, $b: Int) { node { name } }"#;
        let out = strip_unused_variable_declarations(q);
        assert!(
            !out.contains("$a") || !out.contains('('),
            "unused vars should be gone"
        );
        assert!(
            !out.contains("$b") || !out.contains('('),
            "unused vars should be gone"
        );
    }

    #[test]
    fn strip_unused_vars_noop_when_no_variables() {
        let q = r#"{ proposals { id } }"#;
        assert_eq!(strip_unused_variable_declarations(q), q);
    }

    const CHAIN_ID: u64 = 1;

    /// Returns the GraphQL field name for an event on a given chain,
    /// mirroring the SQL table name format.
    fn field_name_for(chain_id: u64, event: &alloy::json_abi::Event) -> String {
        let selector = event.selector().to_string();
        let short_hash = &selector[2..7];
        format!(
            "event_{}_{}_{}",
            chain_id,
            event.name.to_ascii_lowercase(),
            short_hash
        )
    }

    /// Temporary DuckDB file, cleaned up on drop.
    struct TempDb(String);

    impl TempDb {
        fn new(suffix: &str) -> Self {
            Self(format!(
                "/tmp/quixote_graphql_{}_{}.duckdb",
                suffix,
                std::process::id()
            ))
        }
        fn path(&self) -> &str {
            &self.0
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Seeds a DuckDB file, drops the write connection, then reopens read-only as a factory.
    async fn seed_duckdb(
        path: &str,
        event: alloy::json_abi::Event,
        logs: &[alloy::rpc::types::Log],
    ) -> Arc<dyn StorageFactory> {
        {
            let storage = DuckDBStorage::with_db(path).expect("open db");
            storage
                .include_events(CHAIN_ID, &[event])
                .await
                .expect("include events");
            storage
                .add_events(CHAIN_ID, logs)
                .await
                .expect("add events");
        }
        Arc::new(DuckDBStorage::with_db(path).expect("reopen db")) as Arc<dyn StorageFactory>
    }

    // ── DuckDB integration tests ────────────────────────────────────────────

    #[tokio::test]
    async fn duckdb_returns_all_rows() {
        let tmp = TempDb::new("all_rows");
        let logs = LogTestFixture::builder()
            .with_log_count(5)
            .with_erc20_transfer()
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let query = format!(
            "{{ {} {{ blockNumber transactionHash from to value }} }}",
            fname
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 5);
        for row in rows {
            assert!(
                row["blockNumber"].is_string(),
                "blockNumber must be a string"
            );
            assert!(
                row["transactionHash"].is_string(),
                "transactionHash must be a string"
            );
        }
    }

    #[tokio::test]
    async fn duckdb_filters_by_block_range() {
        let tmp = TempDb::new("block_range");
        let logs = LogTestFixture::builder()
            .with_log_count(10)
            .with_start_block(100)
            .with_erc20_transfer()
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        // Blocks 100-109 seeded; querying 103-107 should return exactly 5.
        let query = format!(
            r#"{{ {}(where: {{ blockNumber: {{ gte: "103", lte: "107" }} }}) {{ blockNumber }} }}"#,
            fname
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn duckdb_filters_by_contract_address() {
        let tmp = TempDb::new("contract_addr");
        let addr_a = fake_address();
        let addr_b = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(6)
            .with_erc20_transfer()
            .with_contract_addresses([addr_a.clone(), addr_b.clone()])
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let query = format!(
            r#"{{ {}(where: {{ contractAddress: {{ eq: "{}" }} }}) {{ contractAddress }} }}"#,
            fname, addr_a
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        // 6 logs alternate between two addresses, so 3 belong to addr_a.
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn duckdb_filters_by_param_address() {
        let tmp = TempDb::new("param_addr");
        let sender = fake_address();
        // Single-address pool forces every log to have from = sender.
        let logs = LogTestFixture::builder()
            .with_log_count(4)
            .with_erc20_transfer()
            .with_address_pool([sender.clone()])
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let query = format!(
            r#"{{ {}(where: {{ from: {{ eq: "{}" }} }}) {{ from }} }}"#,
            fname, sender
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 4, "all 4 logs have from = sender");
    }

    #[tokio::test]
    async fn duckdb_filters_by_param_uint256() {
        let tmp = TempDb::new("param_uint");
        let logs = LogTestFixture::builder()
            .with_log_count(5)
            .with_erc20_transfer()
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        // value >= 0 must match all rows (uint256 is always non-negative).
        let query = format!(
            r#"{{ {}(where: {{ value: {{ gte: "0" }} }}) {{ value }} }}"#,
            fname
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn invalid_block_number_filter_returns_error() {
        let tmp = TempDb::new("invalid_block");
        let logs = LogTestFixture::builder().with_erc20_transfer().build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let query = format!(
            r#"{{ {}(where: {{ blockNumber: {{ eq: "not-a-number" }} }}) {{ blockNumber }} }}"#,
            fname
        );
        let res = schema.execute(&query).await;
        assert!(!res.errors.is_empty(), "expected a validation error");
        assert!(
            res.errors[0].message.contains("expected a number"),
            "unexpected error: {}",
            res.errors[0].message
        );
    }

    #[tokio::test]
    async fn unknown_order_by_column_returns_error() {
        let tmp = TempDb::new("bad_orderby");
        let logs = LogTestFixture::builder().with_erc20_transfer().build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let query = format!(
            r#"{{ {}(orderBy: "nonExistentColumn") {{ blockNumber }} }}"#,
            fname
        );
        let res = schema.execute(&query).await;
        assert!(!res.errors.is_empty(), "expected a validation error");
        assert!(
            res.errors[0].message.contains("unknown orderBy column"),
            "unexpected error: {}",
            res.errors[0].message
        );
    }

    #[tokio::test]
    async fn duckdb_condition_exact_match() {
        let tmp = TempDb::new("condition_exact");
        let addr_a = fake_address();
        let addr_b = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(6)
            .with_erc20_transfer()
            .with_contract_addresses([addr_a.clone(), addr_b.clone()])
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let query = format!(
            r#"{{ {}(condition: {{ contractAddress: "{}" }}) {{ contractAddress }} }}"#,
            fname, addr_a
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(
            rows.len(),
            3,
            "condition should match exactly half the logs"
        );
        for row in rows {
            assert_eq!(row["contractAddress"].as_str().unwrap(), addr_a);
        }
    }

    #[tokio::test]
    async fn duckdb_condition_and_where_combine() {
        let tmp = TempDb::new("condition_where");
        let addr_a = fake_address();
        let addr_b = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(10)
            .with_start_block(100)
            .with_erc20_transfer()
            .with_contract_addresses([addr_a.clone(), addr_b.clone()])
            .build();
        let factory = seed_duckdb(tmp.path(), transfer_event(), &logs).await;
        let schema = build_schema_from_factory(factory, None)
            .await
            .expect("build schema");

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        // 10 logs: blocks 100-109, alternating addr_a/addr_b.
        // condition pins addr_a (5 logs: blocks 100,102,104,106,108).
        // where further restricts to blockNumber >= 104 (3 logs: 104,106,108).
        let query = format!(
            r#"{{ {}(condition: {{ contractAddress: "{}" }}, where: {{ blockNumber: {{ gte: "104" }} }}) {{ blockNumber contractAddress }} }}"#,
            fname, addr_a
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row["contractAddress"].as_str().unwrap(), addr_a);
        }
    }

    // ── PostgreSQL integration tests ────────────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn pg_returns_all_rows(pool: sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        use crate::storage::PostgreSqlStorage;
        let storage = PostgreSqlStorage::new(pool);
        let logs = LogTestFixture::builder()
            .with_log_count(5)
            .with_erc20_transfer()
            .build();
        storage
            .include_events(CHAIN_ID, &[transfer_event()])
            .await?;
        storage.add_events(CHAIN_ID, &logs).await?;

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let factory = Arc::new(storage) as Arc<dyn StorageFactory>;
        let schema = build_schema_from_factory(factory, None).await?;

        let query = format!(
            "{{ {} {{ blockNumber transactionHash from to value }} }}",
            fname
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 5);
        for row in rows {
            assert!(
                row["blockNumber"].is_string(),
                "blockNumber must be a string"
            );
            assert!(
                row["transactionHash"].is_string(),
                "transactionHash must be a string"
            );
        }
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pg_filters_by_block_range(pool: sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        use crate::storage::PostgreSqlStorage;
        let storage = PostgreSqlStorage::new(pool);
        let logs = LogTestFixture::builder()
            .with_log_count(10)
            .with_start_block(100)
            .with_erc20_transfer()
            .build();
        storage
            .include_events(CHAIN_ID, &[transfer_event()])
            .await?;
        storage.add_events(CHAIN_ID, &logs).await?;

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let factory = Arc::new(storage) as Arc<dyn StorageFactory>;
        let schema = build_schema_from_factory(factory, None).await?;

        let query = format!(
            r#"{{ {}(where: {{ blockNumber: {{ gte: "103", lte: "107" }} }}) {{ blockNumber }} }}"#,
            fname
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 5);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pg_filters_by_contract_address(
        pool: sqlx::Pool<sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        use crate::storage::PostgreSqlStorage;
        let storage = PostgreSqlStorage::new(pool);
        let addr_a = fake_address();
        let addr_b = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(6)
            .with_erc20_transfer()
            .with_contract_addresses([addr_a.clone(), addr_b.clone()])
            .build();
        storage
            .include_events(CHAIN_ID, &[transfer_event()])
            .await?;
        storage.add_events(CHAIN_ID, &logs).await?;

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let factory = Arc::new(storage) as Arc<dyn StorageFactory>;
        let schema = build_schema_from_factory(factory, None).await?;

        let query = format!(
            r#"{{ {}(where: {{ contractAddress: {{ eq: "{}" }} }}) {{ contractAddress }} }}"#,
            fname, addr_a
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 3);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pg_condition_exact_match(pool: sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        use crate::storage::PostgreSqlStorage;
        let storage = PostgreSqlStorage::new(pool);
        let addr_a = fake_address();
        let addr_b = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(6)
            .with_erc20_transfer()
            .with_contract_addresses([addr_a.clone(), addr_b.clone()])
            .build();
        storage
            .include_events(CHAIN_ID, &[transfer_event()])
            .await?;
        storage.add_events(CHAIN_ID, &logs).await?;

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let factory = Arc::new(storage) as Arc<dyn StorageFactory>;
        let schema = build_schema_from_factory(factory, None).await?;

        let query = format!(
            r#"{{ {}(condition: {{ contractAddress: "{}" }}) {{ contractAddress }} }}"#,
            fname, addr_a
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row["contractAddress"].as_str().unwrap(), addr_a);
        }
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pg_condition_and_where_combine(
        pool: sqlx::Pool<sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        use crate::storage::PostgreSqlStorage;
        let storage = PostgreSqlStorage::new(pool);
        let addr_a = fake_address();
        let addr_b = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(10)
            .with_start_block(100)
            .with_erc20_transfer()
            .with_contract_addresses([addr_a.clone(), addr_b.clone()])
            .build();
        storage
            .include_events(CHAIN_ID, &[transfer_event()])
            .await?;
        storage.add_events(CHAIN_ID, &logs).await?;

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let factory = Arc::new(storage) as Arc<dyn StorageFactory>;
        let schema = build_schema_from_factory(factory, None).await?;

        // condition pins addr_a (5 logs: blocks 100,102,104,106,108).
        // where further restricts to blockNumber >= 104 (3 logs: 104,106,108).
        let query = format!(
            r#"{{ {}(condition: {{ contractAddress: "{}" }}, where: {{ blockNumber: {{ gte: "104" }} }}) {{ blockNumber contractAddress }} }}"#,
            fname, addr_a
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row["contractAddress"].as_str().unwrap(), addr_a);
        }
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pg_where_param_field(pool: sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        use crate::storage::PostgreSqlStorage;
        let storage = PostgreSqlStorage::new(pool);
        let sender = fake_address();
        let logs = LogTestFixture::builder()
            .with_log_count(4)
            .with_erc20_transfer()
            .with_address_pool([sender.clone()])
            .build();
        storage
            .include_events(CHAIN_ID, &[transfer_event()])
            .await?;
        storage.add_events(CHAIN_ID, &logs).await?;

        let fname = field_name_for(CHAIN_ID, &transfer_event());
        let factory = Arc::new(storage) as Arc<dyn StorageFactory>;
        let schema = build_schema_from_factory(factory, None).await?;

        let query = format!(
            r#"{{ {}(where: {{ from: {{ eq: "{}" }} }}) {{ from }} }}"#,
            fname, sender
        );
        let res = schema.execute(&query).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data[&fname].as_array().expect("expected array");
        assert_eq!(rows.len(), 4);
        Ok(())
    }
}
