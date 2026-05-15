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
                    serde_json::Number::from_f64(f)
                        .unwrap_or_else(|| 0i64.into())
                        .into(),
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
                if let Some(where_acc) = ctx.args.get("where") {
                    if let Ok(obj) = where_acc.object() {
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
