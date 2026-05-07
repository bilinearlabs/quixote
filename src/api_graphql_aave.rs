// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! PostGraphile-compatible GraphQL types for the Aave governance page.
//!
//! Exposes:
//!   - `allProposalsViews`    — paginated proposal list
//!   - `getProposalDetail`    — single-proposal detail with all timestamps
//!   - `getProposalVotes`     — per-proposal votes from Polygon + Avalanche
//!   - `getProposalPayloads`  — payloads associated with a proposal
//!
//! All queries back the `aave_*` SQL views created by `setup_sql` in the config.
//! IPFS metadata is fetched on demand and cached for the process lifetime.

use async_graphql::{
    Value as GqlValue,
    dynamic::{
        Enum, EnumItem, Field, FieldFuture, FieldValue, InputValue, Object, SchemaBuilder, TypeRef,
    },
};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::storage::StorageFactory;

const DEFAULT_FIRST: u64 = 10;
const MAX_FIRST: u64 = 1_000;
const IPFS_TIMEOUT_SECS: u64 = 10;
const SHORT_DESC_MAX: usize = 500;

const IPFS_GATEWAY: &str = "https://ipfs.io/ipfs";

// Integer fields shared by ProposalView and ProposalDetail.
// These are registered with TypeRef::INT and returned as JSON numbers.
const PROPOSAL_INT_FIELDS: &[&str] = &["accessLevel", "votingDuration", "stateId"];

const PROPOSAL_VIEW_STRING_FIELDS: &[&str] = &[
    "proposalId",
    "network",
    "creator",
    "ipfsHash",
    "title",
    "author",
    "shortDescription",
    "discussions",
    "snapshotBlockHash",
    "votesFor",
    "votesAgainst",
    "state",
];

const PROPOSAL_DETAIL_STRING_FIELDS: &[&str] = &[
    "proposalId",
    "network",
    "creator",
    "ipfsHash",
    "title",
    "author",
    "shortDescription",
    "description",
    "discussions",
    "snapshotBlockHash",
    "votesFor",
    "votesAgainst",
    "state",
    "createdAt",
    "votingActivatedAt",
    "queuedAt",
    "executedAt",
    "failedAt",
    "cancelledAt",
    "votingStartTime",
    "votingEndTime",
    "l1BlockHash",
    "votingMachineAddress",
    "quorum",
    "requiredDifferential",
    "coolDownBeforeVotingStart",
];

const PROPOSAL_VOTE_FIELDS: &[&str] = &[
    "voter",
    "support",
    "votingPower",
    "votingNetwork",
    "votedAt",
];

const PROPOSAL_PAYLOAD_FIELDS: &[&str] = &[
    "proposalId",
    "payloadId",
    "chainId",
    "payloadsController",
    "creator",
    "maximumAccessLevelRequired",
    "state",
    "createdAt",
    "queuedAt",
    "executedAt",
    "cancelledAt",
];

// ── IPFS types ────────────────────────────────────────────────────────────────

/// Metadata extracted from an IPFS governance proposal document.
#[derive(Clone, Default)]
struct IpfsMetadata {
    title: Option<String>,
    author: Option<String>,
    short_description: Option<String>,
    discussions: Option<String>,
    /// Full markdown body after the YAML front matter (used as `description`).
    description: Option<String>,
}

/// Shared, process-lifetime cache for IPFS proposal metadata.
struct IpfsResolver {
    client: reqwest::Client,
    cache: RwLock<HashMap<String, Arc<IpfsMetadata>>>,
}

impl IpfsResolver {
    fn new() -> Arc<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(IPFS_TIMEOUT_SECS))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Arc::new(Self {
            client,
            cache: RwLock::new(HashMap::new()),
        })
    }

    async fn get(&self, hex_hash: &str) -> Arc<IpfsMetadata> {
        {
            let cache = self.cache.read().await;
            if let Some(meta) = cache.get(hex_hash) {
                return meta.clone();
            }
        }

        let meta = Arc::new(self.fetch(hex_hash).await.unwrap_or_default());

        let mut cache = self.cache.write().await;
        cache
            .entry(hex_hash.to_string())
            .or_insert_with(|| meta.clone());

        meta
    }

    async fn fetch(&self, hex_hash: &str) -> Option<IpfsMetadata> {
        let cid = bytes32_to_cid(hex_hash)?;
        let url = format!("{IPFS_GATEWAY}/{cid}");

        debug!(ipfs_hash = hex_hash, cid = %cid, "fetching IPFS metadata");

        let resp = match self.client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(url, error = %e, "IPFS fetch failed");
                return None;
            }
        };

        let status = resp.status();
        let body = match resp.text().await {
            Ok(t) => t,
            Err(e) => {
                warn!(url, %status, error = %e, "IPFS body read failed");
                return None;
            }
        };

        if let Some(meta) = parse_ipfs_body(&body) {
            return Some(meta);
        }

        warn!(
            url,
            %status,
            body = &body[..body.len().min(200)],
            "IPFS body not parseable"
        );
        None
    }
}

// ── IPFS parsing ──────────────────────────────────────────────────────────────

fn parse_ipfs_body(body: &str) -> Option<IpfsMetadata> {
    let trimmed = body.trim();
    if trimmed.starts_with("---") {
        return Some(parse_front_matter(trimmed));
    }
    None
}

/// Extract the full short description from the markdown body.
///
/// Starts at the first `##` heading and includes all subsequent content
/// (headings and body). No truncation is applied here — callers that need
/// a preview (e.g. `allProposalsViews`) should call `truncate_short_desc`.
fn extract_short_description(md_body: &str) -> Option<String> {
    let start = md_body.find("## ")?;
    Some(md_body[start..].trim_end().to_string())
}

/// Truncate a short description to at most `SHORT_DESC_MAX` Unicode characters.
///
/// Matches Aave's behaviour: hard cut at exactly 500 code points, then `...`.
/// No word-boundary detection — mid-word cuts are intentional.
fn truncate_short_desc(text: &str) -> String {
    let char_count = text.chars().count();
    if char_count <= SHORT_DESC_MAX {
        return text.to_string();
    }
    // Find the byte offset of the 500th character boundary.
    let byte_pos = text
        .char_indices()
        .nth(SHORT_DESC_MAX)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    format!("{}...", &text[..byte_pos])
}

/// Parse a Markdown document with YAML front matter into `IpfsMetadata`.
///
/// Front matter keys: `title`, `author`, `shortDescription`, `discussions`.
/// `shortDescription` falls back to the first `##` section (with headings)
/// if absent, truncated at ~500 characters.
/// `description` is the full markdown body after the closing `---`.
fn parse_front_matter(body: &str) -> IpfsMetadata {
    let rest = body
        .trim_start()
        .strip_prefix("---")
        .unwrap_or(body)
        .trim_start_matches('\n');

    let (fm_block, md_body) = rest
        .find("\n---")
        .map(|i| (&rest[..i], &rest[i + 4..]))
        .unwrap_or((rest, ""));

    let mut map: HashMap<String, String> = HashMap::new();
    for line in fm_block.lines() {
        if let Some((key, val)) = line.split_once(':') {
            let key = key.trim().to_string();
            let val = val.trim().trim_matches('"').trim_matches('\'').to_string();
            map.insert(key, val);
        }
    }

    let short_description = map
        .get("shortDescription")
        .cloned()
        .or_else(|| extract_short_description(md_body));

    let description = if md_body.trim().is_empty() {
        None
    } else {
        Some(md_body.to_string())
    };

    IpfsMetadata {
        title: map.get("title").cloned(),
        author: map.get("author").cloned(),
        short_description,
        discussions: map.get("discussions").cloned(),
        description,
    }
}

/// Convert an on-chain `bytes32` hex string to an IPFS CIDv0 (base58btc).
fn bytes32_to_cid(hex: &str) -> Option<String> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.len() != 64 {
        return None;
    }
    let digest = hex::decode(hex).ok()?;
    let mut multihash = vec![0x12u8, 0x20];
    multihash.extend_from_slice(&digest);
    Some(bs58::encode(multihash).into_string())
}

// ── GraphQL helpers ───────────────────────────────────────────────────────────

fn json_to_gql(v: &serde_json::Value) -> GqlValue {
    match v {
        serde_json::Value::String(s) => GqlValue::String(s.clone()),
        serde_json::Value::Number(n) => GqlValue::String(n.to_string()),
        serde_json::Value::Bool(b) => GqlValue::Boolean(*b),
        serde_json::Value::Null => GqlValue::Null,
        other => GqlValue::String(other.to_string()),
    }
}

/// Build an Object whose fields all return nullable STRING,
/// without registering it yet (so the caller can add more fields).
fn build_string_object(type_name: impl Into<String>, fields: &'static [&'static str]) -> Object {
    let mut obj = Object::new(type_name);
    for &field_name in fields {
        let name = field_name.to_string();
        obj = obj.field(Field::new(
            name.clone(),
            TypeRef::named(TypeRef::STRING),
            move |ctx| {
                let name = name.clone();
                FieldFuture::new(async move {
                    let val = ctx
                        .parent_value
                        .downcast_ref::<serde_json::Value>()
                        .and_then(|v| v.get(&name))
                        .map(json_to_gql);
                    Ok(val.map(FieldValue::value))
                })
            },
        ));
    }
    obj
}

/// Add a nullable INT field to an Object.
///
/// The parent value is expected to be a `serde_json::Value` object.
/// Handles both `Number` and `String` JSON values so the resolver works
/// regardless of how the SQL returns the column.
fn with_int_field(obj: Object, field_name: &'static str) -> Object {
    obj.field(Field::new(
        field_name,
        TypeRef::named(TypeRef::INT),
        move |ctx| {
            FieldFuture::new(async move {
                let gql = ctx
                    .parent_value
                    .downcast_ref::<serde_json::Value>()
                    .and_then(|v| v.get(field_name))
                    .and_then(|v| match v {
                        serde_json::Value::Number(n) => n.as_i64(),
                        serde_json::Value::String(s) => s.parse::<i64>().ok(),
                        _ => None,
                    })
                    .map(|i| GqlValue::Number(i.into()));
                Ok(gql.map(FieldValue::value))
            })
        },
    ))
}

/// Register a connection type with a single `nodes` field.
/// The parent value must be `Vec<serde_json::Value>`.
fn register_connection(
    schema_builder: SchemaBuilder,
    connection_name: impl Into<String>,
    item_type_name: impl Into<String>,
) -> SchemaBuilder {
    let item = item_type_name.into();
    let conn = Object::new(connection_name).field(Field::new(
        "nodes",
        TypeRef::named_nn_list_nn(item),
        |ctx| {
            FieldFuture::new(async move {
                let rows = ctx
                    .parent_value
                    .downcast_ref::<Vec<serde_json::Value>>()
                    .cloned()
                    .unwrap_or_default();
                Ok(Some(FieldValue::list(
                    rows.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    ));
    schema_builder.register(conn)
}

/// Resolve IPFS metadata for every row in `rows` (keyed by `ipfsHash`) and
/// merge the result fields back in-place.
///
/// - `include_description`: when false, the `description` field is skipped
///   (`ProposalView` in `allProposalsViews` does not expose it).
/// - `truncate_sd`: when true, `shortDescription` is clipped to ~500 chars
///   (`allProposalsViews`). Set to false for `getProposalDetail` which needs
///   the full markdown text.
async fn resolve_and_merge_ipfs(
    rows: &mut Vec<serde_json::Value>,
    ipfs: &Arc<IpfsResolver>,
    include_description: bool,
    truncate_sd: bool,
) {
    let hashes: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("ipfsHash").and_then(|v| v.as_str()).map(String::from))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let ipfs_results: HashMap<String, Arc<IpfsMetadata>> = join_all(hashes.iter().map(|h| {
        let resolver = ipfs.clone();
        let hash = h.clone();
        async move { (hash.clone(), resolver.get(&hash).await) }
    }))
    .await
    .into_iter()
    .collect();

    for row in rows.iter_mut() {
        let hash = match row.get("ipfsHash").and_then(|v| v.as_str()) {
            Some(h) => h.to_string(),
            None => continue,
        };
        let meta = match ipfs_results.get(&hash) {
            Some(m) => m,
            None => continue,
        };
        if let Some(obj) = row.as_object_mut() {
            let to_json = |s: &Option<String>| {
                s.as_deref()
                    .map(|v| serde_json::Value::String(v.to_string()))
                    .unwrap_or(serde_json::Value::Null)
            };
            obj.insert("title".into(), to_json(&meta.title));
            obj.insert("author".into(), to_json(&meta.author));
            let sd = meta.short_description.as_deref().map(|s| {
                if truncate_sd {
                    truncate_short_desc(s)
                } else {
                    s.to_string()
                }
            });
            obj.insert(
                "shortDescription".into(),
                sd.map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null),
            );
            obj.insert("discussions".into(), to_json(&meta.discussions));
            if include_description {
                obj.insert("description".into(), to_json(&meta.description));
            }
        }
    }
}

// ── Schema registration ───────────────────────────────────────────────────────

/// Register all Aave governance GraphQL types and query fields.
pub fn register_aave_governance(
    mut schema_builder: SchemaBuilder,
    mut query: Object,
    factory: Arc<dyn StorageFactory>,
) -> (SchemaBuilder, Object) {
    let ipfs = IpfsResolver::new();

    // ── ProposalView ──────────────────────────────────────────────────────────
    {
        let mut obj = build_string_object("ProposalView", PROPOSAL_VIEW_STRING_FIELDS);
        for &f in PROPOSAL_INT_FIELDS {
            obj = with_int_field(obj, f);
        }
        schema_builder = schema_builder.register(obj);
        schema_builder = register_connection(schema_builder, "ProposalsConnection", "ProposalView");
    }

    // ── ProposalDetail ────────────────────────────────────────────────────────
    {
        let mut obj = build_string_object("ProposalDetail", PROPOSAL_DETAIL_STRING_FIELDS);
        for &f in PROPOSAL_INT_FIELDS {
            obj = with_int_field(obj, f);
        }
        schema_builder = schema_builder.register(obj);
        schema_builder =
            register_connection(schema_builder, "ProposalDetailConnection", "ProposalDetail");
    }

    // ── ProposalVote ──────────────────────────────────────────────────────────
    {
        let obj = build_string_object("ProposalVote", PROPOSAL_VOTE_FIELDS);
        schema_builder = schema_builder.register(obj);
        schema_builder = register_connection(schema_builder, "VotesConnection", "ProposalVote");
    }

    // ── ProposalPayload ───────────────────────────────────────────────────────
    {
        let obj = build_string_object("ProposalPayload", PROPOSAL_PAYLOAD_FIELDS);
        schema_builder = schema_builder.register(obj);
        schema_builder =
            register_connection(schema_builder, "PayloadsConnection", "ProposalPayload");
    }

    // ── Enum ──────────────────────────────────────────────────────────────────
    let order_by_enum = Enum::new("ProposalOrderBy")
        .item(EnumItem::new("PROPOSAL_ID_DESC"))
        .item(EnumItem::new("PROPOSAL_ID_ASC"));
    schema_builder = schema_builder.register(order_by_enum);

    // ── allProposalsViews ─────────────────────────────────────────────────────
    {
        let factory_c = factory.clone();
        let ipfs_c = ipfs.clone();
        query = query.field(
            Field::new(
                "allProposalsViews",
                TypeRef::named_nn("ProposalsConnection"),
                move |ctx| {
                    let factory = factory_c.clone();
                    let ipfs = ipfs_c.clone();
                    FieldFuture::new(async move {
                        let first = ctx
                            .args
                            .get("first")
                            .and_then(|v| v.i64().ok())
                            .map(|n| (n as u64).min(MAX_FIRST))
                            .unwrap_or(DEFAULT_FIRST);
                        let offset = ctx
                            .args
                            .get("offset")
                            .and_then(|v| v.i64().ok())
                            .map(|n| n as u64)
                            .unwrap_or(0);
                        let order_dir = ctx
                            .args
                            .get("orderBy")
                            .map(|v| match v.as_value() {
                                GqlValue::Enum(name) if name.as_str() == "PROPOSAL_ID_ASC" => {
                                    "ASC"
                                }
                                _ => "DESC",
                            })
                            .unwrap_or("DESC");

                        // accessLevel, votingDuration, stateId are returned as
                        // INT so pg_col_to_json emits JSON numbers, and
                        // with_int_field resolvers pick them up correctly.
                        let sql = format!(
                            r#"SELECT
  CAST("proposalId" AS TEXT)          AS "proposalId",
  'ethereum'                           AS network,
  "creator",
  CAST("accessLevel" AS INT)          AS "accessLevel",
  SUBSTRING("ipfsHash" FROM 3)        AS "ipfsHash",
  CAST(NULL AS TEXT)                  AS title,
  CAST(NULL AS TEXT)                  AS author,
  CAST(NULL AS TEXT)                  AS "shortDescription",
  CAST(NULL AS TEXT)                  AS discussions,
  "snapshotBlockHash",
  CAST("votingDuration" AS INT)       AS "votingDuration",
  CAST("votesFor" AS TEXT)            AS "votesFor",
  CAST("votesAgainst" AS TEXT)        AS "votesAgainst",
  state,
  "stateId"
FROM aave_proposals
ORDER BY "proposalId" {order_dir}
LIMIT {first} OFFSET {offset}"#
                        );

                        let storage = factory
                            .create_storage()
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                        let result = storage
                            .send_raw_query(&sql)
                            .await
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        let mut rows: Vec<serde_json::Value> = match result {
                            serde_json::Value::Array(arr) => arr,
                            _ => vec![],
                        };

                        resolve_and_merge_ipfs(&mut rows, &ipfs, false, true).await;

                        Ok(Some(FieldValue::owned_any(rows)))
                    })
                },
            )
            .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
            .argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)))
            .argument(InputValue::new("orderBy", TypeRef::named("ProposalOrderBy"))),
        );
    }

    // ── getProposalDetail ─────────────────────────────────────────────────────
    {
        let factory_c = factory.clone();
        let ipfs_c = ipfs.clone();
        query = query.field(
            Field::new(
                "getProposalDetail",
                TypeRef::named_nn("ProposalDetailConnection"),
                move |ctx| {
                    let factory = factory_c.clone();
                    let ipfs = ipfs_c.clone();
                    FieldFuture::new(async move {
                        let proposal_id: u64 = ctx
                            .args
                            .get("pProposalId")
                            .and_then(|v| v.string().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .ok_or_else(|| {
                                async_graphql::Error::new(
                                    "pProposalId is required and must be a valid integer",
                                )
                            })?;

                        let sql = format!(
                            r#"SELECT
  CAST(pc."proposalId" AS TEXT)                                                   AS "proposalId",
  'ethereum'                                                                       AS network,
  pc."creator",
  CAST(pc."accessLevel" AS INT)                                                   AS "accessLevel",
  SUBSTRING(pc."ipfsHash" FROM 3)                                                 AS "ipfsHash",
  CAST(NULL AS TEXT)                                                               AS title,
  CAST(NULL AS TEXT)                                                               AS author,
  CAST(NULL AS TEXT)                                                               AS "shortDescription",
  CAST(NULL AS TEXT)                                                               AS description,
  CAST(NULL AS TEXT)                                                               AS discussions,
  va."snapshotBlockHash",
  COALESCE(CAST(va."votingDuration" AS INT), 259200)                             AS "votingDuration",
  COALESCE(pq."votesFor", pf."votesFor", ve_agg."liveVotesFor"::TEXT, '0')       AS "votesFor",
  COALESCE(pq."votesAgainst", pf."votesAgainst", ve_agg."liveVotesAgainst"::TEXT, '0') AS "votesAgainst",
  CASE
      WHEN pcan."proposalId" IS NOT NULL THEN 'cancelled'
      WHEN pex."proposalId"  IS NOT NULL THEN 'executed'
      WHEN pf."proposalId"   IS NOT NULL THEN 'failed'
      WHEN pq."proposalId"   IS NOT NULL THEN 'queued'
      WHEN va."proposalId"   IS NOT NULL THEN 'active'
      ELSE 'created'
  END AS state,
  CASE
      WHEN pcan."proposalId" IS NOT NULL THEN 6
      WHEN pex."proposalId"  IS NOT NULL THEN 4
      WHEN pf."proposalId"   IS NOT NULL THEN 5
      WHEN pq."proposalId"   IS NOT NULL THEN 3
      WHEN va."proposalId"   IS NOT NULL THEN 2
      ELSE 1
  END AS "stateId",
  CAST(b_pc.block_timestamp   AS TEXT)                                            AS "createdAt",
  CAST(b_va.block_timestamp   AS TEXT)                                            AS "votingActivatedAt",
  CAST(b_pq.block_timestamp   AS TEXT)                                            AS "queuedAt",
  CAST(b_pex.block_timestamp  AS TEXT)                                            AS "executedAt",
  CAST(b_pf.block_timestamp   AS TEXT)                                            AS "failedAt",
  CAST(b_pcan.block_timestamp AS TEXT)                                            AS "cancelledAt",
  CAST(b_va.block_timestamp   AS TEXT)                                            AS "votingStartTime",
  CAST(
      CASE WHEN b_va.block_timestamp IS NOT NULL
           THEN b_va.block_timestamp + COALESCE(CAST(va."votingDuration" AS NUMERIC), 259200)
           ELSE NULL END
  AS TEXT)                                                                        AS "votingEndTime",
  va."snapshotBlockHash"                                                           AS "l1BlockHash",
  CAST(NULL AS TEXT)                                                               AS "votingMachineAddress",
  CAST(NULL AS TEXT)                                                               AS quorum,
  CAST(NULL AS TEXT)                                                               AS "requiredDifferential",
  CAST(NULL AS TEXT)                                                               AS "coolDownBeforeVotingStart"
FROM event_1_proposalcreated_cc914 pc
LEFT JOIN blocks_1 b_pc    ON b_pc.block_number   = pc.block_number
LEFT JOIN event_1_votingactivated_45f1d  va   ON va."proposalId"   = pc."proposalId"
LEFT JOIN blocks_1 b_va    ON b_va.block_number   = va.block_number
LEFT JOIN event_1_proposalqueued_e39e7   pq   ON pq."proposalId"   = pc."proposalId"
LEFT JOIN blocks_1 b_pq    ON b_pq.block_number   = pq.block_number
LEFT JOIN event_1_proposalfailed_2bed8   pf   ON pf."proposalId"   = pc."proposalId"
LEFT JOIN blocks_1 b_pf    ON b_pf.block_number   = pf.block_number
LEFT JOIN event_1_proposalexecuted_712ae pex  ON pex."proposalId"  = pc."proposalId"
LEFT JOIN blocks_1 b_pex   ON b_pex.block_number  = pex.block_number
LEFT JOIN event_1_proposalcanceled_789cf pcan ON pcan."proposalId" = pc."proposalId"
LEFT JOIN blocks_1 b_pcan  ON b_pcan.block_number = pcan.block_number
LEFT JOIN (
    SELECT
        "proposalId",
        SUM(CASE WHEN "support" = 'true'  THEN "votingPower" ELSE 0 END) AS "liveVotesFor",
        SUM(CASE WHEN "support" = 'false' THEN "votingPower" ELSE 0 END) AS "liveVotesAgainst"
    FROM (
        SELECT "proposalId", "support", "votingPower" FROM event_1_voteemitted_0c611
        UNION ALL
        SELECT "proposalId", "support", "votingPower" FROM event_137_voteemitted_0c611
        UNION ALL
        SELECT "proposalId", "support", "votingPower" FROM event_43114_voteemitted_0c611
    ) ve
    GROUP BY "proposalId"
) ve_agg ON ve_agg."proposalId" = pc."proposalId"
WHERE pc."proposalId" = {proposal_id}"#
                        );

                        let storage = factory
                            .create_storage()
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                        let result = storage
                            .send_raw_query(&sql)
                            .await
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        let mut rows: Vec<serde_json::Value> = match result {
                            serde_json::Value::Array(arr) => arr,
                            _ => vec![],
                        };

                        resolve_and_merge_ipfs(&mut rows, &ipfs, true, false).await;

                        Ok(Some(FieldValue::owned_any(rows)))
                    })
                },
            )
            .argument(InputValue::new("pProposalId", TypeRef::named_nn(TypeRef::STRING))),
        );
    }

    // ── getProposalVotes ──────────────────────────────────────────────────────
    //
    // Votes come from VoteEmitted on Polygon (137) and Avalanche (43114).
    // VoteForwarded on Ethereum is a different event that does not carry
    // per-voter votingPower, so it is not included here.
    {
        let factory_c = factory.clone();
        query = query.field(
            Field::new(
                "getProposalVotes",
                TypeRef::named_nn("VotesConnection"),
                move |ctx| {
                    let factory = factory_c.clone();
                    FieldFuture::new(async move {
                        let proposal_id: u64 = ctx
                            .args
                            .get("pProposalId")
                            .and_then(|v| v.string().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .ok_or_else(|| {
                                async_graphql::Error::new(
                                    "pProposalId is required and must be a valid integer",
                                )
                            })?;

                        let limit = ctx
                            .args
                            .get("limitCount")
                            .and_then(|v| v.i64().ok())
                            .map(|n| (n as u64).min(MAX_FIRST))
                            .unwrap_or(DEFAULT_FIRST);
                        let offset = ctx
                            .args
                            .get("offsetCount")
                            .and_then(|v| v.i64().ok())
                            .map(|n| n as u64)
                            .unwrap_or(0);

                        // Optional boolean filter; stored in DB as TEXT 'true'/'false'.
                        let support_where = ctx
                            .args
                            .get("pSupport")
                            .and_then(|v| v.boolean().ok())
                            .map(|b| format!("WHERE \"support\" = '{b}'"))
                            .unwrap_or_default();

                        let sql = format!(
                            r#"SELECT voter, support, "votingPower", "votingNetwork",
       CAST("votedAt" AS TEXT) AS "votedAt"
FROM (
    SELECT
        "voter",
        "support",
        CAST("votingPower" AS TEXT)  AS "votingPower",
        'polygon'                    AS "votingNetwork",
        b.block_timestamp            AS "votedAt"
    FROM event_137_voteemitted_0c611 v
    LEFT JOIN blocks_137 b ON b.block_number = v.block_number
    WHERE v."proposalId" = {proposal_id}
    UNION ALL
    SELECT
        "voter",
        "support",
        CAST("votingPower" AS TEXT)  AS "votingPower",
        'avalanche'                  AS "votingNetwork",
        b.block_timestamp            AS "votedAt"
    FROM event_43114_voteemitted_0c611 v
    LEFT JOIN blocks_43114 b ON b.block_number = v.block_number
    WHERE v."proposalId" = {proposal_id}
) votes
{support_where}
ORDER BY "votedAt" DESC
LIMIT {limit} OFFSET {offset}"#
                        );

                        let storage = factory
                            .create_storage()
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                        let result = storage
                            .send_raw_query(&sql)
                            .await
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        let rows: Vec<serde_json::Value> = match result {
                            serde_json::Value::Array(arr) => arr,
                            _ => vec![],
                        };

                        Ok(Some(FieldValue::owned_any(rows)))
                    })
                },
            )
            .argument(InputValue::new("pProposalId", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("pSupport", TypeRef::named(TypeRef::BOOLEAN)))
            .argument(InputValue::new("limitCount", TypeRef::named(TypeRef::INT)))
            .argument(InputValue::new("offsetCount", TypeRef::named(TypeRef::INT))),
        );
    }

    // ── getProposalPayloads ───────────────────────────────────────────────────
    //
    // Only `proposalId`, `payloadId`, `chainId`, and `payloadsController` are
    // available from the indexed PayloadSent event. The remaining fields
    // (creator, maximumAccessLevelRequired, state, *At timestamps) require
    // payload-controller events on each L2 which are not currently indexed;
    // they are returned as null.
    {
        let factory_c = factory.clone();
        query = query.field(
            Field::new(
                "getProposalPayloads",
                TypeRef::named_nn("PayloadsConnection"),
                move |ctx| {
                    let factory = factory_c.clone();
                    FieldFuture::new(async move {
                        let proposal_id: u64 = ctx
                            .args
                            .get("pProposalId")
                            .and_then(|v| v.string().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .ok_or_else(|| {
                                async_graphql::Error::new(
                                    "pProposalId is required and must be a valid integer",
                                )
                            })?;

                        let sql = format!(
                            r#"SELECT
  CAST("proposalId" AS TEXT)     AS "proposalId",
  "payloadId",
  CAST("chainId" AS TEXT)        AS "chainId",
  "payloadsController",
  CAST(NULL AS TEXT)             AS creator,
  CAST(NULL AS TEXT)             AS "maximumAccessLevelRequired",
  CAST(NULL AS TEXT)             AS state,
  CAST(NULL AS TEXT)             AS "createdAt",
  CAST(NULL AS TEXT)             AS "queuedAt",
  CAST(NULL AS TEXT)             AS "executedAt",
  CAST(NULL AS TEXT)             AS "cancelledAt"
FROM aave_payloads
WHERE "proposalId" = {proposal_id}
ORDER BY "payloadNumberOnProposal" ASC"#
                        );

                        let storage = factory
                            .create_storage()
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                        let result = storage
                            .send_raw_query(&sql)
                            .await
                            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        let rows: Vec<serde_json::Value> = match result {
                            serde_json::Value::Array(arr) => arr,
                            _ => vec![],
                        };

                        Ok(Some(FieldValue::owned_any(rows)))
                    })
                },
            )
            .argument(InputValue::new("pProposalId", TypeRef::named_nn(TypeRef::STRING))),
        );
    }

    (schema_builder, query)
}
