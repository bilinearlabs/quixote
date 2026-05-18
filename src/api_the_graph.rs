// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Generic GraphQL engine driven by an SDL schema file and a config-defined
//! entity/view mapping. Implements the subset of The Graph's query API needed
//! for entity queries with filters, ordering, pagination, and nested selection.

use crate::configuration::{GraphqlEntityConfig, GraphqlLayerConfig, GraphqlQueryConfig};
use crate::storage::StorageFactory;
use async_graphql::dynamic::{Enum, EnumItem, Field, FieldFuture, FieldValue, InputObject, InputValue, Object, SchemaBuilder, TypeRef};
use async_graphql_parser::{
    parse_schema,
    types::{BaseType, TypeKind, TypeSystemDefinition},
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ── SQL helpers ──────────────────────────────────────────────────────────────

/// Always produces a quoted string literal safe for any column type.
/// PostgreSQL implicitly casts quoted literals to numeric/boolean as needed.
fn gql_to_sql_literal(v: &async_graphql::Value) -> Option<String> {
    match v {
        async_graphql::Value::Number(n) => Some(format!("'{}'", n)),
        async_graphql::Value::String(s) => Some(format!("'{}'", s.replace('\'', "''"))),
        async_graphql::Value::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}

fn normalize_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}

/// Always quotes the key so it works for both text and numeric columns.
fn key_to_sql_literal(k: &str) -> String {
    format!("'{}'", k.replace('\'', "''"))
}

// ── Storage helpers ──────────────────────────────────────────────────────────

async fn run_query(
    factory: &Arc<dyn StorageFactory>,
    sql: &str,
) -> Result<Vec<Value>, async_graphql::Error> {
    let storage = factory
        .create_storage()
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
    let result = storage
        .send_raw_query(sql)
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
    match result {
        Value::Array(arr) => Ok(arr),
        _ => Ok(vec![]),
    }
}

// ── Relation pre-fetching and merging ────────────────────────────────────────

async fn prefetch_and_merge(
    factory: &Arc<dyn StorageFactory>,
    mut rows: Vec<Value>,
    entity: &GraphqlEntityConfig,
) -> Result<Vec<Value>, async_graphql::Error> {
    for rel in &entity.relations {
        let parent_col = rel.parent_column.as_deref().unwrap_or(&entity.id_column);

        let keys: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get(parent_col))
            .map(normalize_key)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if keys.is_empty() {
            continue;
        }

        let in_list = keys
            .iter()
            .map(|k| key_to_sql_literal(k))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"SELECT * FROM "{}" WHERE "{}" IN ({})"#,
            rel.view, rel.foreign_key, in_list
        );

        let children = run_query(factory, &sql).await?;

        let mut by_key: HashMap<String, Vec<Value>> = HashMap::new();
        for child in children {
            let key = child
                .get(&rel.foreign_key)
                .map(normalize_key)
                .unwrap_or_default();
            by_key.entry(key).or_default().push(child);
        }

        for row in &mut rows {
            if let Value::Object(map) = row {
                let parent_key = map.get(parent_col).map(normalize_key).unwrap_or_default();
                let children = by_key.get(&parent_key).cloned().unwrap_or_default();
                if rel.singleton {
                    let child_val = children.into_iter().next().unwrap_or(Value::Null);
                    map.insert(rel.field.clone(), child_val);
                } else {
                    map.insert(rel.field.clone(), Value::Array(children));
                }
            }
        }
    }
    Ok(rows)
}

// ── Value conversion ─────────────────────────────────────────────────────────

/// Convert a serde_json Value to a GQL value, coercing to `sdl_type` when the
/// raw column type doesn't match. NUMERIC columns come back as `Value::String`
/// from pg_col_to_json; Int/Boolean SDL fields need explicit parsing.
fn coerce_json_to_gql(v: &Value, sdl_type: &str) -> async_graphql::Value {
    match sdl_type {
        "Int" => match v {
            Value::Number(n) => n
                .as_i64()
                .map(|i| async_graphql::Value::Number(i.into()))
                .unwrap_or(async_graphql::Value::Null),
            Value::String(s) => s
                .parse::<i64>()
                .map(|i| async_graphql::Value::Number(i.into()))
                .unwrap_or(async_graphql::Value::Null),
            Value::Null => async_graphql::Value::Null,
            _ => async_graphql::Value::Null,
        },
        "Boolean" => match v {
            Value::Bool(b) => async_graphql::Value::Boolean(*b),
            Value::String(s) => match s.as_str() {
                "true" | "TRUE" | "t" | "1" => async_graphql::Value::Boolean(true),
                _ => async_graphql::Value::Boolean(false),
            },
            Value::Null => async_graphql::Value::Null,
            _ => async_graphql::Value::Null,
        },
        _ => json_to_gql(v),
    }
}

fn json_to_gql(v: &Value) -> async_graphql::Value {
    match v {
        Value::Null => async_graphql::Value::Null,
        Value::Bool(b) => async_graphql::Value::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                async_graphql::Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                async_graphql::Value::Number(
                    serde_json::Number::from_f64(f).unwrap_or_else(|| 0i64.into()),
                )
            } else {
                async_graphql::Value::Null
            }
        }
        Value::String(s) => async_graphql::Value::String(s.clone()),
        Value::Array(arr) => async_graphql::Value::List(arr.iter().map(json_to_gql).collect()),
        Value::Object(map) => {
            let fields = map
                .iter()
                .map(|(k, v)| (async_graphql::Name::new(k), json_to_gql(v)))
                .collect();
            async_graphql::Value::Object(fields)
        }
    }
}

// ── Field resolver builders ──────────────────────────────────────────────────

fn scalar_field(gql_name: String, json_key: String, tr: TypeRef, sdl_base: String) -> Field {
    Field::new(gql_name, tr, move |ctx| {
        let key = json_key.clone();
        let sdl_base = sdl_base.clone();
        FieldFuture::new(async move {
            let v = ctx
                .parent_value
                .downcast_ref::<Value>()
                .and_then(|obj| obj.get(&key));
            Ok(v.map(|val| FieldValue::value(coerce_json_to_gql(val, &sdl_base))))
        })
    })
}

fn object_field(gql_name: String, tr: TypeRef) -> Field {
    let key = gql_name.clone();
    Field::new(gql_name, tr, move |ctx| {
        let key = key.clone();
        FieldFuture::new(async move {
            let val = ctx
                .parent_value
                .downcast_ref::<Value>()
                .and_then(|obj| obj.get(&key))
                .filter(|v| !v.is_null())
                .cloned();
            Ok(val.map(FieldValue::owned_any))
        })
    })
}

fn list_object_field(gql_name: String, tr: TypeRef) -> Field {
    let key = gql_name.clone();
    Field::new(gql_name, tr, move |ctx| {
        let key = key.clone();
        FieldFuture::new(async move {
            let arr = ctx
                .parent_value
                .downcast_ref::<Value>()
                .and_then(|obj| obj.get(&key))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            Ok(Some(FieldValue::list(
                arr.into_iter().map(FieldValue::owned_any),
            )))
        })
    })
}

// ── SDL helpers ──────────────────────────────────────────────────────────────

fn innermost_name(ty: &async_graphql_parser::types::Type) -> &str {
    match &ty.base {
        BaseType::Named(n) => n.as_str(),
        BaseType::List(inner) => innermost_name(inner),
    }
}

fn sdl_type_ref(ty: &async_graphql_parser::types::Type) -> TypeRef {
    match &ty.base {
        BaseType::Named(name) => {
            if ty.nullable {
                TypeRef::named(name.as_str())
            } else {
                TypeRef::named_nn(name.as_str())
            }
        }
        BaseType::List(inner) => {
            let inner_name = innermost_name(inner);
            match (ty.nullable, inner.nullable) {
                (true, true) => TypeRef::named_list(inner_name),
                (true, false) => TypeRef::named_list_nn(inner_name),
                (false, true) => TypeRef::named_nn_list(inner_name),
                (false, false) => TypeRef::named_nn_list_nn(inner_name),
            }
        }
    }
}

// ── Query builders ───────────────────────────────────────────────────────────

fn build_list_query(
    entity: GraphqlEntityConfig,
    qcfg: GraphqlQueryConfig,
    factory: Arc<dyn StorageFactory>,
) -> Field {
    let type_name = entity.type_name.clone();
    let filter_type_name = format!("{}_filter", entity.type_name);
    let orderby_type_name = format!("{}_orderBy", entity.type_name);
    let has_filters = !qcfg.filters.is_empty();
    let has_orderby = !qcfg.order_by_fields.is_empty();
    let filter_defs = qcfg.filters;

    let mut field = Field::new(
        qcfg.name,
        TypeRef::named_nn_list_nn(&type_name),
        move |ctx| {
            let entity = entity.clone();
            let filter_defs = filter_defs.clone();
            let factory = factory.clone();
            FieldFuture::new(async move {
                let mut conditions: Vec<String> = vec![];

                // Parse `where` input object
                if let Some(where_acc) = ctx.args.get("where")
                    && let Ok(obj) = where_acc.object()
                {
                    for f in &filter_defs {
                        if let Some(val_acc) = obj.get(&f.field) {
                            if f.field.ends_with("_in") {
                                // List/IN filter
                                if let Ok(list) = val_acc.list() {
                                    let lits: Vec<String> = list
                                        .iter()
                                        .filter_map(|v| gql_to_sql_literal(v.as_value()))
                                        .collect();
                                    if !lits.is_empty() {
                                        conditions.push(format!(
                                            r#""{}" IN ({})"#,
                                            f.column,
                                            lits.join(", ")
                                        ));
                                    }
                                }
                            } else if let Some(lit) = gql_to_sql_literal(val_acc.as_value()) {
                                conditions.push(format!(r#""{}" = {}"#, f.column, lit));
                            }
                        } else if f.required {
                            return Err(async_graphql::Error::new(format!(
                                "{} is required",
                                f.field
                            )));
                        }
                    }
                }

                let first = ctx
                    .args
                    .get("first")
                    .and_then(|v| v.i64().ok())
                    .map(|n| n.min(1000))
                    .unwrap_or(100);
                let skip = ctx
                    .args
                    .get("skip")
                    .and_then(|v| v.i64().ok())
                    .unwrap_or(0);

                // orderBy: accept enum value or string
                let order_col = ctx
                    .args
                    .get("orderBy")
                    .map(|v| match v.as_value() {
                        async_graphql::Value::Enum(n) => n.as_str().to_string(),
                        async_graphql::Value::String(s) => s.clone(),
                        _ => entity.id_column.clone(),
                    })
                    .unwrap_or_else(|| entity.id_column.clone());
                // Sanitize: allow only alphanumeric and underscore
                let safe_col: String = order_col
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let safe_col = if safe_col.is_empty() {
                    entity.id_column.clone()
                } else {
                    safe_col
                };

                let order_dir = ctx
                    .args
                    .get("orderDirection")
                    .map(|v| match v.as_value() {
                        async_graphql::Value::Enum(n) => {
                            if n.as_str().eq_ignore_ascii_case("asc") {
                                "ASC"
                            } else {
                                "DESC"
                            }
                        }
                        async_graphql::Value::String(s) => {
                            if s.eq_ignore_ascii_case("asc") { "ASC" } else { "DESC" }
                        }
                        _ => "DESC",
                    })
                    .unwrap_or("DESC");

                let where_clause = if conditions.is_empty() {
                    String::new()
                } else {
                    format!("WHERE {}", conditions.join(" AND "))
                };
                let sql = format!(
                    r#"SELECT * FROM "{}" {} ORDER BY "{}" {} LIMIT {} OFFSET {}"#,
                    entity.view, where_clause, safe_col, order_dir, first, skip
                );

                let rows = run_query(&factory, &sql).await?;
                let merged = prefetch_and_merge(&factory, rows, &entity).await?;
                Ok(Some(FieldValue::list(
                    merged.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("skip", TypeRef::named(TypeRef::INT)));

    if has_filters {
        field = field.argument(InputValue::new("where", TypeRef::named(&filter_type_name)));
    }
    if has_orderby {
        field = field
            .argument(InputValue::new("orderBy", TypeRef::named(&orderby_type_name)))
            .argument(InputValue::new(
                "orderDirection",
                TypeRef::named("OrderDirection"),
            ));
    }
    field
}

fn build_single_query(
    entity: GraphqlEntityConfig,
    qcfg: GraphqlQueryConfig,
    factory: Arc<dyn StorageFactory>,
) -> Field {
    let type_name = entity.type_name.clone();
    Field::new(
        qcfg.name,
        TypeRef::named(&type_name),
        move |ctx| {
            let entity = entity.clone();
            let factory = factory.clone();
            FieldFuture::new(async move {
                let id = ctx
                    .args
                    .get("id")
                    .map(|v| match v.as_value() {
                        async_graphql::Value::String(s) => s.clone(),
                        async_graphql::Value::Number(n) => n.to_string(),
                        async_graphql::Value::Enum(n) => n.as_str().to_string(),
                        other => other.to_string(),
                    })
                    .ok_or_else(|| async_graphql::Error::new("id is required"))?;

                let id_lit = key_to_sql_literal(&id);
                let sql = format!(
                    r#"SELECT * FROM "{}" WHERE "{}" = {} LIMIT 1"#,
                    entity.view, entity.id_column, id_lit
                );

                let rows = run_query(&factory, &sql).await?;
                let mut merged = prefetch_and_merge(&factory, rows, &entity).await?;
                Ok(merged.pop().map(FieldValue::owned_any))
            })
        },
    )
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
}

fn build_batch_query(
    entity: GraphqlEntityConfig,
    qcfg: GraphqlQueryConfig,
    factory: Arc<dyn StorageFactory>,
) -> Field {
    let type_name = entity.type_name.clone();
    Field::new(
        qcfg.name,
        TypeRef::named_nn_list_nn(&type_name),
        move |ctx| {
            let entity = entity.clone();
            let factory = factory.clone();
            FieldFuture::new(async move {
                let ids: Vec<String> = ctx
                    .args
                    .get("ids")
                    .and_then(|v| v.list().ok())
                    .map(|list| {
                        list.iter()
                            .filter_map(|v| v.string().ok().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();

                if ids.is_empty() {
                    return Ok(Some(FieldValue::list(Vec::<FieldValue>::new())));
                }

                let in_list = ids
                    .iter()
                    .map(|s| key_to_sql_literal(s))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    r#"SELECT * FROM "{}" WHERE "{}" IN ({}) ORDER BY "{}" DESC"#,
                    entity.view, entity.id_column, in_list, entity.id_column
                );

                let rows = run_query(&factory, &sql).await?;
                let merged = prefetch_and_merge(&factory, rows, &entity).await?;
                Ok(Some(FieldValue::list(
                    merged.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "ids",
        TypeRef::named_nn_list_nn(TypeRef::ID),
    ))
}

fn build_search_query(
    entity: GraphqlEntityConfig,
    qcfg: GraphqlQueryConfig,
    factory: Arc<dyn StorageFactory>,
) -> Field {
    let type_name = entity.type_name.clone();
    let search_column = qcfg
        .column
        .clone()
        .unwrap_or_else(|| entity.id_column.clone());

    Field::new(
        qcfg.name,
        TypeRef::named_nn_list_nn(&type_name),
        move |ctx| {
            let entity = entity.clone();
            let search_column = search_column.clone();
            let factory = factory.clone();
            FieldFuture::new(async move {
                let text = ctx
                    .args
                    .get("text")
                    .and_then(|v| v.string().ok().map(str::to_string))
                    .ok_or_else(|| async_graphql::Error::new("text is required"))?;
                let first = ctx
                    .args
                    .get("first")
                    .and_then(|v| v.i64().ok())
                    .map(|n| n.min(1000))
                    .unwrap_or(100);

                // Allow only alphanumeric + hyphen/underscore to prevent injection
                let safe_text: String = text
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
                    .collect();
                if safe_text.is_empty() {
                    return Ok(Some(FieldValue::list(Vec::<FieldValue>::new())));
                }

                let sql = format!(
                    r#"SELECT * FROM "{}" WHERE CAST("{}" AS TEXT) LIKE '%{}%' ORDER BY "{}" DESC LIMIT {}"#,
                    entity.view, search_column, safe_text, entity.id_column, first
                );

                let rows = run_query(&factory, &sql).await?;
                let merged = prefetch_and_merge(&factory, rows, &entity).await?;
                Ok(Some(FieldValue::list(
                    merged.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new("text", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
}

// ── Main registration ────────────────────────────────────────────────────────

/// Register all entity types and query fields from the given config into the schema builder.
///
/// Reads the SDL schema file, parses it, and for each entity defined in `graphql_config`:
/// - Registers a dynamic Object type with field resolvers backed by `serde_json::Value`
/// - Registers query fields of types `list`, `single`, `batch`, or `search`
/// - Pre-fetches and merges related rows via the `relations` config entries
///
/// Non-entity object types in the SDL (used as nested return values) are also registered.
pub fn register_the_graph_entities(
    mut schema_builder: SchemaBuilder,
    mut query: Object,
    graphql_config: &GraphqlLayerConfig,
    factory: Arc<dyn StorageFactory>,
) -> Result<(SchemaBuilder, Object), String> {
    let schema_src = std::fs::read_to_string(&graphql_config.schema)
        .map_err(|e| format!("failed to read schema '{}': {e}", graphql_config.schema))?;

    let doc = parse_schema::<String>(schema_src)
        .map_err(|e| format!("SDL parse error: {e}"))?;

    // Collect all SDL object types and identify which are @entity
    type FieldList = Vec<async_graphql_parser::Positioned<async_graphql_parser::types::FieldDefinition>>;
    let mut entity_fields: HashMap<String, FieldList> = HashMap::new();
    let mut nested_fields: HashMap<String, FieldList> = HashMap::new();
    let mut object_type_names: HashSet<String> = HashSet::new();

    for def in &doc.definitions {
        if let TypeSystemDefinition::Type(td) = def {
            let name = td.node.name.node.as_str().to_string();
            if let TypeKind::Object(obj_type) = &td.node.kind {
                object_type_names.insert(name.clone());
                let is_entity = td
                    .node
                    .directives
                    .iter()
                    .any(|d| d.node.name.node.as_str() == "entity");
                if is_entity {
                    entity_fields.insert(name, obj_type.fields.clone());
                } else {
                    nested_fields.insert(name, obj_type.fields.clone());
                }
            }
        }
    }

    // Entity config lookup by type name
    let entity_by_type: HashMap<&str, &GraphqlEntityConfig> = graphql_config
        .entities
        .iter()
        .map(|e| (e.type_name.as_str(), e))
        .collect();

    // Register nested (non-entity) object types — simple "read from parent Value" resolvers
    for (type_name, fields) in &nested_fields {
        let mut obj = Object::new(type_name.as_str());
        for pf in fields {
            let fd = &pf.node;
            let gql_name = fd.name.node.as_str().to_string();
            let base_name = innermost_name(&fd.ty.node).to_string();
            let is_list = matches!(fd.ty.node.base, BaseType::List(_));
            let tr = sdl_type_ref(&fd.ty.node);

            if object_type_names.contains(&base_name) {
                if is_list {
                    obj = obj.field(list_object_field(gql_name, tr));
                } else {
                    obj = obj.field(object_field(gql_name, tr));
                }
            } else {
                obj = obj.field(scalar_field(gql_name.clone(), gql_name, tr, base_name));
            }
        }
        schema_builder = schema_builder.register(obj);
    }

    // Register entity object types
    for (type_name, fields) in &entity_fields {
        let id_column = entity_by_type
            .get(type_name.as_str())
            .map(|e| e.id_column.as_str())
            .unwrap_or("id");

        let mut obj = Object::new(type_name.as_str());
        for pf in fields {
            let fd = &pf.node;
            let gql_name = fd.name.node.as_str().to_string();
            let base_name = innermost_name(&fd.ty.node).to_string();
            let is_list = matches!(fd.ty.node.base, BaseType::List(_));
            let tr = sdl_type_ref(&fd.ty.node);

            if object_type_names.contains(&base_name) {
                if is_list {
                    obj = obj.field(list_object_field(gql_name, tr));
                } else {
                    obj = obj.field(object_field(gql_name, tr));
                }
            } else {
                let json_key = if gql_name == "id" {
                    id_column.to_string()
                } else {
                    gql_name.clone()
                };
                obj = obj.field(scalar_field(gql_name, json_key, tr, base_name));
            }
        }
        schema_builder = schema_builder.register(obj);
    }

    // Register global OrderDirection enum (used by all list queries that have order_by_fields)
    let order_direction = Enum::new("OrderDirection")
        .item(EnumItem::new("asc"))
        .item(EnumItem::new("desc"));
    schema_builder = schema_builder.register(order_direction);

    // Register per-entity filter input types and orderBy enums for list queries
    for entity in &graphql_config.entities {
        let mut registered_filter = false;
        let mut registered_orderby = false;
        for qcfg in &entity.queries {
            if qcfg.query_type != "list" {
                continue;
            }
            // Register {EntityName}_filter input type (once per entity)
            if !qcfg.filters.is_empty() && !registered_filter {
                let filter_type_name = format!("{}_filter", entity.type_name);
                let mut filter_input = InputObject::new(&filter_type_name);
                for f in &qcfg.filters {
                    let tr = if f.field.ends_with("_in") {
                        let item = match f.arg_type.as_str() {
                            "Int" => TypeRef::INT,
                            "Boolean" => TypeRef::BOOLEAN,
                            _ => TypeRef::STRING,
                        };
                        TypeRef::named_list(item)
                    } else {
                        match f.arg_type.as_str() {
                            "Int" => TypeRef::named(TypeRef::INT),
                            "Boolean" => TypeRef::named(TypeRef::BOOLEAN),
                            _ => TypeRef::named(TypeRef::STRING),
                        }
                    };
                    filter_input = filter_input.field(InputValue::new(f.field.as_str(), tr));
                }
                schema_builder = schema_builder.register(filter_input);
                registered_filter = true;
            }
            // Register {EntityName}_orderBy enum (once per entity)
            if !qcfg.order_by_fields.is_empty() && !registered_orderby {
                let orderby_type_name = format!("{}_orderBy", entity.type_name);
                let mut orderby_enum = Enum::new(&orderby_type_name);
                for col in &qcfg.order_by_fields {
                    orderby_enum = orderby_enum.item(EnumItem::new(col.as_str()));
                }
                schema_builder = schema_builder.register(orderby_enum);
                registered_orderby = true;
            }
        }
    }

    // Register query fields for each configured entity
    for entity in &graphql_config.entities {
        for qcfg in &entity.queries {
            let field = match qcfg.query_type.as_str() {
                "list" => build_list_query(entity.clone(), qcfg.clone(), factory.clone()),
                "single" => build_single_query(entity.clone(), qcfg.clone(), factory.clone()),
                "batch" => build_batch_query(entity.clone(), qcfg.clone(), factory.clone()),
                "search" => build_search_query(entity.clone(), qcfg.clone(), factory.clone()),
                other => {
                    return Err(format!(
                        "unknown query type '{}' for entity '{}'",
                        other, entity.type_name
                    ))
                }
            };
            query = query.field(field);
        }
    }

    Ok((schema_builder, query))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{
        GraphqlEntityConfig, GraphqlFilterConfig, GraphqlLayerConfig, GraphqlQueryConfig,
    };
    use crate::storage::{
        ContractDescriptorDb, EventDescriptorDb, EventQuery, Storage, StorageFactory,
    };
    use crate::EventStatus;
    use alloy::{json_abi::Event as AbiEvent, primitives::B256, rpc::types::Log};
    use anyhow::Result;
    use async_graphql::dynamic::{Object, Schema};
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::sync::Arc;

    // ── Mock storage ──────────────────────────────────────────────────────────

    struct MockStorage {
        rows: Vec<Value>,
    }

    #[async_trait]
    impl Storage for MockStorage {
        async fn send_raw_query(&self, _sql: &str) -> Result<Value> {
            Ok(Value::Array(self.rows.clone()))
        }
        async fn add_events(&self, _: u64, _: &[Log]) -> Result<()> { unimplemented!() }
        async fn list_indexed_events(&self) -> Result<Vec<EventDescriptorDb>> { Ok(vec![]) }
        async fn event_index_status(&self, _: u64, _: &AbiEvent) -> Result<Option<EventStatus>> { unimplemented!() }
        async fn include_events(&self, _: u64, _: &[AbiEvent]) -> Result<()> { unimplemented!() }
        async fn get_event_signature(&self, _: &str) -> Result<String> { unimplemented!() }
        async fn last_block(&self, _: u64, _: &AbiEvent) -> Result<u64> { unimplemented!() }
        async fn first_block(&self, _: u64, _: &AbiEvent) -> Result<u64> { unimplemented!() }
        async fn set_first_block(&self, _: u64, _: &AbiEvent, _: u64) -> Result<()> { unimplemented!() }
        async fn synchronize_events(&self, _: u64, _: &[B256], _: Option<u64>) -> Result<()> { unimplemented!() }
        async fn run_setup_sql(&self, _: &[String]) -> Result<()> { unimplemented!() }
        async fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>> { unimplemented!() }
        async fn describe_database(&self) -> Result<Value> { unimplemented!() }
        async fn query_event_table(&self, _: &EventQuery) -> Result<Vec<Value>> { unimplemented!() }
    }

    struct MockFactory {
        rows: Vec<Value>,
    }

    impl MockFactory {
        fn with_rows(rows: Vec<Value>) -> Arc<dyn StorageFactory> {
            Arc::new(Self { rows })
        }
        fn empty() -> Arc<dyn StorageFactory> {
            Arc::new(Self { rows: vec![] })
        }
    }

    impl StorageFactory for MockFactory {
        fn create_storage(&self) -> Result<Box<dyn Storage>> {
            Ok(Box::new(MockStorage { rows: self.rows.clone() }))
        }
    }

    // ── SDL temp file ─────────────────────────────────────────────────────────

    struct TempSdl(String);

    impl TempSdl {
        fn new(suffix: &str, content: &str) -> Self {
            let path = format!(
                "/tmp/quixote_schema_{}_{}.graphql",
                suffix,
                std::process::id()
            );
            std::fs::write(&path, content).expect("write SDL file");
            Self(path)
        }
        fn path(&self) -> &str {
            &self.0
        }
    }

    impl Drop for TempSdl {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // ── SDL and config helpers ────────────────────────────────────────────────

    const TOKEN_SDL: &str = r#"
type Token @entity {
  id: ID!
  name: String!
  symbol: String!
}
"#;

    fn token_config(schema_path: &str) -> GraphqlLayerConfig {
        GraphqlLayerConfig {
            schema: schema_path.to_string(),
            entities: vec![GraphqlEntityConfig {
                type_name: "Token".to_string(),
                view: "tokens".to_string(),
                id_column: "id".to_string(),
                queries: vec![
                    GraphqlQueryConfig {
                        name: "tokens".to_string(),
                        query_type: "list".to_string(),
                        filters: vec![
                            GraphqlFilterConfig {
                                field: "symbol".to_string(),
                                column: "symbol".to_string(),
                                required: false,
                                arg_type: "String".to_string(),
                            },
                            GraphqlFilterConfig {
                                field: "id_in".to_string(),
                                column: "id".to_string(),
                                required: false,
                                arg_type: "String".to_string(),
                            },
                        ],
                        column: None,
                        order_by_fields: vec!["id".to_string(), "name".to_string()],
                    },
                    GraphqlQueryConfig {
                        name: "token".to_string(),
                        query_type: "single".to_string(),
                        filters: vec![],
                        column: None,
                        order_by_fields: vec![],
                    },
                    GraphqlQueryConfig {
                        name: "tokensByIds".to_string(),
                        query_type: "batch".to_string(),
                        filters: vec![],
                        column: None,
                        order_by_fields: vec![],
                    },
                    GraphqlQueryConfig {
                        name: "tokenSearch".to_string(),
                        query_type: "search".to_string(),
                        filters: vec![],
                        column: Some("name".to_string()),
                        order_by_fields: vec![],
                    },
                ],
                relations: vec![],
            }],
        }
    }

    fn make_schema(sdl_path: &str, rows: Vec<Value>) -> Schema {
        let cfg = token_config(sdl_path);
        let factory = MockFactory::with_rows(rows);
        let (sb, query) = register_the_graph_entities(
            Schema::build("Query", None, None),
            Object::new("Query"),
            &cfg,
            factory,
        )
        .expect("register entities");
        sb.register(query).finish().expect("build schema")
    }

    // ── Unit tests for helper functions ───────────────────────────────────────

    #[test]
    fn gql_to_sql_literal_escapes_single_quotes_in_strings() {
        let v = async_graphql::Value::String("it's alive".to_string());
        assert_eq!(gql_to_sql_literal(&v), Some("'it''s alive'".to_string()));
    }

    #[test]
    fn gql_to_sql_literal_wraps_numbers_in_quotes() {
        let v = async_graphql::Value::Number(42i64.into());
        assert_eq!(gql_to_sql_literal(&v), Some("'42'".to_string()));
    }

    #[test]
    fn gql_to_sql_literal_renders_booleans_unquoted() {
        assert_eq!(
            gql_to_sql_literal(&async_graphql::Value::Boolean(true)),
            Some("true".to_string())
        );
        assert_eq!(
            gql_to_sql_literal(&async_graphql::Value::Boolean(false)),
            Some("false".to_string())
        );
    }

    #[test]
    fn gql_to_sql_literal_returns_none_for_null() {
        assert_eq!(gql_to_sql_literal(&async_graphql::Value::Null), None);
    }

    #[test]
    fn coerce_int_parses_number_value() {
        assert_eq!(
            coerce_json_to_gql(&json!(42), "Int"),
            async_graphql::Value::Number(42i64.into())
        );
    }

    #[test]
    fn coerce_int_parses_string_value() {
        assert_eq!(
            coerce_json_to_gql(&json!("99"), "Int"),
            async_graphql::Value::Number(99i64.into())
        );
    }

    #[test]
    fn coerce_int_returns_null_for_non_numeric_string() {
        assert_eq!(
            coerce_json_to_gql(&json!("not-a-number"), "Int"),
            async_graphql::Value::Null
        );
    }

    #[test]
    fn coerce_boolean_from_bool_value() {
        assert_eq!(
            coerce_json_to_gql(&json!(true), "Boolean"),
            async_graphql::Value::Boolean(true)
        );
    }

    #[test]
    fn coerce_boolean_from_string_true_variants() {
        for s in &["true", "TRUE", "t", "1"] {
            assert_eq!(
                coerce_json_to_gql(&json!(s), "Boolean"),
                async_graphql::Value::Boolean(true),
                "expected true for '{s}'"
            );
        }
    }

    #[test]
    fn coerce_boolean_from_string_false_returns_false() {
        assert_eq!(
            coerce_json_to_gql(&json!("false"), "Boolean"),
            async_graphql::Value::Boolean(false)
        );
    }

    #[test]
    fn coerce_string_type_passes_through() {
        assert_eq!(
            coerce_json_to_gql(&json!("hello"), "String"),
            async_graphql::Value::String("hello".to_string())
        );
    }

    // ── Schema registration tests ─────────────────────────────────────────────

    #[test]
    fn register_entities_fails_on_missing_schema_file() {
        let cfg = GraphqlLayerConfig {
            schema: "/tmp/__nonexistent_schema_quixote__.graphql".to_string(),
            entities: vec![],
        };
        let result = register_the_graph_entities(
            Schema::build("Query", None, None),
            Object::new("Query"),
            &cfg,
            MockFactory::empty(),
        );
        match result {
            Err(e) => assert!(e.contains("failed to read schema"), "unexpected error: {e}"),
            Ok(_) => panic!("expected an error for missing schema file"),
        }
    }

    #[test]
    fn register_entities_fails_on_invalid_sdl() {
        let sdl = TempSdl::new("invalid_sdl", "not valid graphql !!!");
        let cfg = GraphqlLayerConfig {
            schema: sdl.path().to_string(),
            entities: vec![],
        };
        let result = register_the_graph_entities(
            Schema::build("Query", None, None),
            Object::new("Query"),
            &cfg,
            MockFactory::empty(),
        );
        assert!(result.is_err(), "should fail for unparseable SDL");
    }

    #[test]
    fn register_entities_succeeds_and_exposes_entity_type_in_sdl() {
        let sdl = TempSdl::new("valid_sdl", TOKEN_SDL);
        let cfg = token_config(sdl.path());
        let (sb, query) = register_the_graph_entities(
            Schema::build("Query", None, None),
            Object::new("Query"),
            &cfg,
            MockFactory::empty(),
        )
        .expect("register should succeed");
        let schema = sb.register(query).finish().expect("schema should build");
        let schema_sdl = schema.sdl();
        assert!(schema_sdl.contains("Token"), "Token type must appear in schema SDL");
    }

    // ── Resolver tests with mock storage ─────────────────────────────────────

    #[tokio::test]
    async fn list_query_returns_all_rows_from_storage() {
        let sdl = TempSdl::new("list", TOKEN_SDL);
        let schema = make_schema(
            sdl.path(),
            vec![
                json!({"id": "1", "name": "Ethereum", "symbol": "ETH"}),
                json!({"id": "2", "name": "Bitcoin", "symbol": "BTC"}),
            ],
        );

        let res = schema.execute("{ tokens { id name symbol } }").await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data["tokens"].as_array().expect("array");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"].as_str().unwrap(), "1");
        assert_eq!(rows[1]["symbol"].as_str().unwrap(), "BTC");
    }

    #[tokio::test]
    async fn list_query_returns_empty_list_when_storage_is_empty() {
        let sdl = TempSdl::new("list_empty", TOKEN_SDL);
        let schema = make_schema(sdl.path(), vec![]);

        let res = schema.execute("{ tokens { id } }").await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert!(data["tokens"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn single_query_returns_first_row_from_storage() {
        let sdl = TempSdl::new("single", TOKEN_SDL);
        let schema = make_schema(
            sdl.path(),
            vec![json!({"id": "42", "name": "Ether", "symbol": "ETH"})],
        );

        let res = schema.execute(r#"{ token(id: "42") { id name } }"#).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert_eq!(data["token"]["id"].as_str().unwrap(), "42");
        assert_eq!(data["token"]["name"].as_str().unwrap(), "Ether");
    }

    #[tokio::test]
    async fn single_query_returns_null_when_storage_is_empty() {
        let sdl = TempSdl::new("single_null", TOKEN_SDL);
        let schema = make_schema(sdl.path(), vec![]);

        let res = schema.execute(r#"{ token(id: "999") { id } }"#).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert!(data["token"].is_null());
    }

    #[tokio::test]
    async fn batch_query_returns_rows_from_storage() {
        let sdl = TempSdl::new("batch", TOKEN_SDL);
        let schema = make_schema(
            sdl.path(),
            vec![
                json!({"id": "1", "name": "Ethereum", "symbol": "ETH"}),
                json!({"id": "3", "name": "USDC", "symbol": "USDC"}),
            ],
        );

        let res = schema
            .execute(r#"{ tokensByIds(ids: ["1", "3"]) { id symbol } }"#)
            .await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data["tokensByIds"].as_array().expect("array");
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn batch_query_with_empty_ids_returns_empty() {
        let sdl = TempSdl::new("batch_empty", TOKEN_SDL);
        let schema = make_schema(sdl.path(), vec![]);

        let res = schema.execute(r#"{ tokensByIds(ids: []) { id } }"#).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert!(data["tokensByIds"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn search_query_returns_rows_from_storage() {
        let sdl = TempSdl::new("search", TOKEN_SDL);
        let schema = make_schema(
            sdl.path(),
            vec![
                json!({"id": "1", "name": "Ethereum", "symbol": "ETH"}),
                json!({"id": "2", "name": "EthereumClassic", "symbol": "ETC"}),
            ],
        );

        let res = schema
            .execute(r#"{ tokenSearch(text: "Ethereum") { id name } }"#)
            .await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        let rows = data["tokenSearch"].as_array().expect("array");
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn search_query_with_only_special_chars_returns_empty_without_querying() {
        let sdl = TempSdl::new("search_special", TOKEN_SDL);
        // Even with rows in storage, filtering on special-only text is caught before SQL
        let schema = make_schema(
            sdl.path(),
            vec![json!({"id": "1", "name": "Ethereum", "symbol": "ETH"})],
        );

        let res = schema.execute(r#"{ tokenSearch(text: "!!!") { id } }"#).await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert!(data["tokenSearch"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_query_passes_pagination_arguments_to_storage() {
        let sdl = TempSdl::new("pagination", TOKEN_SDL);
        // Mock returns exactly the rows it has regardless of SQL, so we just verify no errors
        // and pagination args are accepted without complaint.
        let schema = make_schema(
            sdl.path(),
            vec![json!({"id": "2", "name": "Beta", "symbol": "B"})],
        );

        let res = schema
            .execute(r#"{ tokens(first: 2, skip: 1, orderBy: id, orderDirection: asc) { id } }"#)
            .await;
        assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
        let data = res.data.into_json().unwrap();
        assert!(data["tokens"].as_array().is_some());
    }
}
