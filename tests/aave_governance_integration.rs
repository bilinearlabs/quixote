// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Integration tests for Aave Governance V3 event indexing.
//!
//! Contract: 0x9AEE0B04504CeF83A65AC3f0e838D0593BCb2BC7 (proxy)
//! Implementation: 0x58BcB647C4bEff253B4B6996c62F737B783f2cDd
//!
//! Covered events:
//! - ProposalCreated   (3 indexed + 1 non-indexed bytes32)
//! - VotingActivated   (2 indexed + 1 non-indexed uint24)
//! - ProposalQueued    (1 indexed + 2 non-indexed uint128)
//! - ProposalFailed    (1 indexed + 2 non-indexed uint128)
//! - ProposalExecuted  (1 indexed, no data)
//! - ProposalCanceled  (1 indexed, no data)
//! - PayloadSent       (3 indexed + 3 non-indexed)
//! - VoteForwarded     (3 indexed + 1 non-indexed dynamic array)
//!
//! The tests also exercise `run_setup_sql` by creating an `aave_proposals` view
//! that derives proposal state by joining the lifecycle event tables.

use alloy::json_abi::Event;
use alloy::rpc::types::Log;
use quixote::api_graphql::build_schema_from_factory;
use quixote::storage::{DuckDBStorage, Storage, StorageFactory};
use serde_json::json;
use std::sync::Arc;

const CHAIN_ID: u64 = 1;
const GOVERNANCE_CONTRACT: &str = "0x9aee0b04504cef83a65ac3f0e838d0593bcb2bc7";
const CREATOR_ADDRESS_TOPIC: &str =
    "0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef12";
const PAYLOADS_CONTROLLER_TOPIC: &str =
    "0x000000000000000000000000deaddeaddeaddeaddeaddeaddeaddeaddeaddead";
const VOTER_ADDRESS_TOPIC: &str =
    "0x000000000000000000000000cafe000000000000000000000000000000000001";

// ── event constructors ────────────────────────────────────────────────────────

fn proposal_created_event() -> Event {
    Event::parse(
        "ProposalCreated(uint256 indexed proposalId, address indexed creator, uint8 indexed accessLevel, bytes32 ipfsHash)",
    )
    .expect("failed to parse ProposalCreated")
}

fn voting_activated_event() -> Event {
    Event::parse(
        "VotingActivated(uint256 indexed proposalId, bytes32 indexed snapshotBlockHash, uint24 votingDuration)",
    )
    .expect("failed to parse VotingActivated")
}

fn proposal_queued_event() -> Event {
    Event::parse(
        "ProposalQueued(uint256 indexed proposalId, uint128 votesFor, uint128 votesAgainst)",
    )
    .expect("failed to parse ProposalQueued")
}

fn proposal_failed_event() -> Event {
    Event::parse(
        "ProposalFailed(uint256 indexed proposalId, uint128 votesFor, uint128 votesAgainst)",
    )
    .expect("failed to parse ProposalFailed")
}

fn proposal_executed_event() -> Event {
    Event::parse("ProposalExecuted(uint256 indexed proposalId)")
        .expect("failed to parse ProposalExecuted")
}

fn proposal_canceled_event() -> Event {
    Event::parse("ProposalCanceled(uint256 indexed proposalId)")
        .expect("failed to parse ProposalCanceled")
}

fn payload_sent_event() -> Event {
    Event::parse(
        "PayloadSent(uint256 indexed proposalId, uint40 payloadId, address indexed payloadsController, uint256 indexed chainId, uint256 payloadNumberOnProposal, uint256 numberOfPayloadsOnProposal)",
    )
    .expect("failed to parse PayloadSent")
}

fn vote_emitted_event() -> Event {
    Event::parse(
        "VoteEmitted(uint256 indexed proposalId, address indexed voter, bool indexed support, uint256 votingPower)",
    )
    .expect("failed to parse VoteEmitted")
}

fn vote_forwarded_event() -> Event {
    // The non-indexed param is an array of structs; alloy represents struct arrays as
    // tuple arrays: (address underlyingAsset, uint128 slot)[].
    // All fields we care about (proposalId, voter, support) are indexed.
    Event::parse(
        "VoteForwarded(uint256 indexed proposalId, address indexed voter, bool indexed support, (address,uint128)[] votingAssetsWithSlot)",
    )
    .expect("failed to parse VoteForwarded")
}

fn all_governance_events() -> Vec<Event> {
    vec![
        proposal_created_event(),
        voting_activated_event(),
        proposal_queued_event(),
        proposal_failed_event(),
        proposal_executed_event(),
        proposal_canceled_event(),
        payload_sent_event(),
        vote_forwarded_event(),
    ]
}

// ── SQL views that materialise Aave's composite entity model ──────────────────
//
// Table names are deterministic: event_{chain_id}_{event_name_lowercase}_{short_hash}
// where short_hash = first 5 hex chars of the event selector (keccak256 of canonical sig).
//
// To inspect them at any time: SELECT event_name, event_hash FROM event_descriptor
// The short hash is event_hash[2..7].
//
// State priority (highest wins):
//   5 Canceled > 4 Executed > 3 Failed > 2 Queued > 1 VotingActivated > 0 Created
//
// block_timestamp columns come from blocks_1 (populated by the indexer).
// In tests logs carry no timestamp, so block_timestamp defaults to 0.
const PROPOSALS_VIEW_SQL: &str = r#"
CREATE OR REPLACE VIEW aave_proposals AS
SELECT
    pc."proposalId",
    pc."creator",
    pc."accessLevel",
    pc."ipfsHash",
    va."snapshotBlockHash",
    va."votingDuration",
    COALESCE(pq."votesFor",     pf."votesFor")     AS "votesFor",
    COALESCE(pq."votesAgainst", pf."votesAgainst") AS "votesAgainst",
    CASE
        WHEN pcan."proposalId" IS NOT NULL THEN 'Canceled'
        WHEN pex."proposalId"  IS NOT NULL THEN 'Executed'
        WHEN pf."proposalId"   IS NOT NULL THEN 'Failed'
        WHEN pq."proposalId"   IS NOT NULL THEN 'Queued'
        WHEN va."proposalId"   IS NOT NULL THEN 'VotingActivated'
        ELSE 'Created'
    END AS state,
    CASE
        WHEN pcan."proposalId" IS NOT NULL THEN 5
        WHEN pex."proposalId"  IS NOT NULL THEN 4
        WHEN pf."proposalId"   IS NOT NULL THEN 3
        WHEN pq."proposalId"   IS NOT NULL THEN 2
        WHEN va."proposalId"   IS NOT NULL THEN 1
        ELSE 0
    END AS "stateId",
    b_pc.block_timestamp   AS "createdAt",
    b_va.block_timestamp   AS "votingActivatedAt",
    COALESCE(b_pq.block_timestamp, b_pf.block_timestamp) AS "closedAt",
    b_pex.block_timestamp  AS "executedAt",
    b_pcan.block_timestamp AS "canceledAt"
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
"#;

const PAYLOADS_VIEW_SQL: &str = r#"
CREATE OR REPLACE VIEW aave_payloads AS
SELECT
    ps."proposalId",
    ps."payloadId",
    ps."payloadsController",
    ps."chainId",
    ps."payloadNumberOnProposal",
    ps."numberOfPayloadsOnProposal",
    ps.block_number,
    b.block_timestamp AS "sentAt"
FROM event_1_payloadsent_9c687 ps
LEFT JOIN blocks_1 b ON b.block_number = ps.block_number
"#;

const VOTES_VIEW_SQL: &str = r#"
CREATE OR REPLACE VIEW aave_votes AS
SELECT
    vf."proposalId",
    vf."voter",
    vf."support",
    vf.block_number,
    vf.transaction_hash,
    b.block_timestamp AS "votedAt"
FROM event_1_voteforwarded_f78ab vf
LEFT JOIN blocks_1 b ON b.block_number = vf.block_number
"#;

// ── log builders ─────────────────────────────────────────────────────────────

fn make_log(
    event: &Event,
    extra_topics: &[String],
    data: &str,
    block: u64,
    tx_idx: u64,
    log_idx: u64,
) -> Log {
    let mut topics = vec![event.selector().to_string()];
    topics.extend_from_slice(extra_topics);

    serde_json::from_value(json!({
        "address": GOVERNANCE_CONTRACT,
        "topics": topics,
        "data": data,
        "blockNumber": format!("0x{:x}", block),
        "transactionHash": format!("0x{:064x}", block * 1000 + tx_idx),
        "transactionIndex": format!("0x{:x}", tx_idx),
        "blockHash": format!("0x{:064x}", block),
        "logIndex": format!("0x{:x}", log_idx),
        "removed": false
    }))
    .expect("failed to build log")
}

fn proposal_created_log(proposal_id: u64, block: u64, log_idx: u64) -> Log {
    make_log(
        &proposal_created_event(),
        &[
            format!("0x{:064x}", proposal_id),
            CREATOR_ADDRESS_TOPIC.to_string(),
            format!("0x{:064x}", 1u8), // accessLevel = 1
        ],
        "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        block,
        0,
        log_idx,
    )
}

fn voting_activated_log(proposal_id: u64, block: u64, log_idx: u64) -> Log {
    make_log(
        &voting_activated_event(),
        &[
            format!("0x{:064x}", proposal_id),
            "0xdeadbeef00000000000000000000000000000000000000000000000000000001".to_string(),
        ],
        &format!("0x{:064x}", 604800u32), // 7 days in seconds
        block,
        0,
        log_idx,
    )
}

fn proposal_queued_log(
    proposal_id: u64,
    votes_for: u128,
    votes_against: u128,
    block: u64,
    log_idx: u64,
) -> Log {
    make_log(
        &proposal_queued_event(),
        &[format!("0x{:064x}", proposal_id)],
        &format!("0x{:064x}{:064x}", votes_for, votes_against),
        block,
        0,
        log_idx,
    )
}

fn proposal_failed_log(
    proposal_id: u64,
    votes_for: u128,
    votes_against: u128,
    block: u64,
    log_idx: u64,
) -> Log {
    make_log(
        &proposal_failed_event(),
        &[format!("0x{:064x}", proposal_id)],
        &format!("0x{:064x}{:064x}", votes_for, votes_against),
        block,
        0,
        log_idx,
    )
}

fn proposal_executed_log(proposal_id: u64, block: u64, log_idx: u64) -> Log {
    make_log(
        &proposal_executed_event(),
        &[format!("0x{:064x}", proposal_id)],
        "0x",
        block,
        0,
        log_idx,
    )
}

fn proposal_canceled_log(proposal_id: u64, block: u64, log_idx: u64) -> Log {
    make_log(
        &proposal_canceled_event(),
        &[format!("0x{:064x}", proposal_id)],
        "0x",
        block,
        0,
        log_idx,
    )
}

fn payload_sent_log(proposal_id: u64, payload_id: u64, block: u64, log_idx: u64) -> Log {
    make_log(
        &payload_sent_event(),
        &[
            format!("0x{:064x}", proposal_id),
            PAYLOADS_CONTROLLER_TOPIC.to_string(),
            format!("0x{:064x}", 1u64), // chainId = 1
        ],
        &format!("0x{:064x}{:064x}{:064x}", payload_id, 0u64, 1u64),
        block,
        0,
        log_idx,
    )
}

fn vote_emitted_log(
    proposal_id: u64,
    support: bool,
    voting_power: u128,
    block: u64,
    log_idx: u64,
) -> Log {
    make_log(
        &vote_emitted_event(),
        &[
            format!("0x{:064x}", proposal_id),
            VOTER_ADDRESS_TOPIC.to_string(),
            format!("0x{:064x}", support as u8),
        ],
        &format!("0x{:064x}", voting_power),
        block,
        0,
        log_idx,
    )
}

fn vote_forwarded_log(proposal_id: u64, support: bool, block: u64, log_idx: u64) -> Log {
    // ABI encoding for an empty (address,uint128)[] array:
    //   offset to array data  = 0x20 (32 bytes)
    //   array length          = 0
    let data = "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000";
    make_log(
        &vote_forwarded_event(),
        &[
            format!("0x{:064x}", proposal_id),
            VOTER_ADDRESS_TOPIC.to_string(),
            format!("0x{:064x}", support as u8),
        ],
        data,
        block,
        0,
        log_idx,
    )
}

// ── helpers ───────────────────────────────────────────────────────────────────

struct TempDb(String);

impl TempDb {
    fn new(suffix: &str) -> Self {
        Self(format!(
            "/tmp/quixote_gov_{}_{}.duckdb",
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

async fn seed_governance_db(path: &str, events: &[Event], logs: &[Log]) -> Arc<dyn StorageFactory> {
    {
        let storage = DuckDBStorage::with_db(path).expect("open db");
        storage
            .include_events(CHAIN_ID, events)
            .await
            .expect("include events");
        storage
            .add_events(CHAIN_ID, logs)
            .await
            .expect("add events");
    }
    Arc::new(DuckDBStorage::with_db(path).expect("reopen db")) as Arc<dyn StorageFactory>
}

// Table name formula: event_{chain_id}_{event_name_lowercase}_{short_hash}
// short_hash = first 5 hex chars of the event selector after "0x"
fn event_table_name(chain_id: u64, event: &Event) -> String {
    let selector = event.selector().to_string();
    let short_hash = &selector[2..7];
    format!(
        "event_{}_{}_{}",
        chain_id,
        event.name.to_ascii_lowercase(),
        short_hash
    )
}

async fn count_stored(storage: &DuckDBStorage, event: &Event) -> usize {
    let table = event_table_name(CHAIN_ID, event);
    let result = storage
        .send_raw_query(&format!("SELECT COUNT(*) AS n FROM {table}"))
        .await
        .expect("count query failed");
    result
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|row| row.get("n"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn all_governance_events_can_be_registered() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();

    let indexed = storage.list_indexed_events().await.unwrap();
    assert_eq!(
        indexed.len(),
        8,
        "all 8 governance events must be registered"
    );
    for entry in &indexed {
        assert_eq!(entry.event_count, Some(0), "no logs stored yet");
    }
}

#[tokio::test]
async fn proposal_created_log_is_stored_with_correct_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[proposal_created_event()])
        .await
        .unwrap();

    storage
        .add_events(CHAIN_ID, &[proposal_created_log(42, 17_500_000, 0)])
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &proposal_created_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &proposal_created_event());
    let rows = storage
        .send_raw_query(&format!(
            r#"SELECT "proposalId", "creator", "accessLevel", "ipfsHash" FROM {table}"#
        ))
        .await
        .unwrap();

    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "42");
    assert_eq!(
        row["creator"].as_str().unwrap().to_lowercase(),
        "0xabcdef1234567890abcdef1234567890abcdef12"
    );
    assert_eq!(row["accessLevel"], "1");
    assert_eq!(
        row["ipfsHash"].as_str().unwrap().to_lowercase(),
        "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    );
}

#[tokio::test]
async fn voting_activated_log_is_stored_with_correct_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[voting_activated_event()])
        .await
        .unwrap();
    storage
        .add_events(CHAIN_ID, &[voting_activated_log(42, 17_500_001, 0)])
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &voting_activated_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &voting_activated_event());
    let rows = storage
        .send_raw_query(&format!(
            r#"SELECT "proposalId", "votingDuration" FROM {table}"#
        ))
        .await
        .unwrap();
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "42");
    assert_eq!(row["votingDuration"], "604800");
}

#[tokio::test]
async fn proposal_queued_log_is_stored_with_correct_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[proposal_queued_event()])
        .await
        .unwrap();
    storage
        .add_events(
            CHAIN_ID,
            &[proposal_queued_log(5, 1_000_000, 250_000, 17_500_002, 0)],
        )
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &proposal_queued_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &proposal_queued_event());
    let rows = storage
        .send_raw_query(&format!(
            r#"SELECT "proposalId", "votesFor", "votesAgainst" FROM {table}"#
        ))
        .await
        .unwrap();
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "5");
    assert_eq!(row["votesFor"], "1000000");
    assert_eq!(row["votesAgainst"], "250000");
}

#[tokio::test]
async fn proposal_executed_log_is_stored() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[proposal_executed_event()])
        .await
        .unwrap();
    storage
        .add_events(CHAIN_ID, &[proposal_executed_log(10, 17_500_010, 0)])
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &proposal_executed_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &proposal_executed_event());
    let rows = storage
        .send_raw_query(&format!(r#"SELECT "proposalId" FROM {table}"#))
        .await
        .unwrap();
    assert_eq!(rows.as_array().unwrap()[0]["proposalId"], "10");
}

#[tokio::test]
async fn payload_sent_log_is_stored_with_correct_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[payload_sent_event()])
        .await
        .unwrap();
    storage
        .add_events(CHAIN_ID, &[payload_sent_log(3, 7, 17_500_020, 0)])
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &payload_sent_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &payload_sent_event());
    let rows = storage
        .send_raw_query(&format!(
            r#"SELECT "proposalId", "payloadId", "chainId", "payloadNumberOnProposal", "numberOfPayloadsOnProposal" FROM {table}"#
        ))
        .await
        .unwrap();
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "3");
    assert_eq!(row["payloadId"], "7");
    assert_eq!(row["chainId"], "1");
    assert_eq!(row["payloadNumberOnProposal"], "0");
    assert_eq!(row["numberOfPayloadsOnProposal"], "1");
}

#[tokio::test]
async fn vote_forwarded_log_is_stored_with_correct_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[vote_forwarded_event()])
        .await
        .unwrap();
    storage
        .add_events(CHAIN_ID, &[vote_forwarded_log(5, true, 17_500_030, 0)])
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &vote_forwarded_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &vote_forwarded_event());
    let rows = storage
        .send_raw_query(&format!(
            r#"SELECT "proposalId", "voter", "support" FROM {table}"#
        ))
        .await
        .unwrap();
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "5");
    assert_eq!(
        row["voter"].as_str().unwrap().to_lowercase(),
        "0xcafe000000000000000000000000000000000001"
    );
    assert_eq!(row["support"], "true");
}

// ── setup_sql / state view tests ──────────────────────────────────────────────

#[tokio::test]
async fn run_setup_sql_creates_view() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();

    storage
        .run_setup_sql(&[
            PROPOSALS_VIEW_SQL.to_string(),
            PAYLOADS_VIEW_SQL.to_string(),
            VOTES_VIEW_SQL.to_string(),
        ])
        .await
        .unwrap();

    // All three views must be queryable immediately.
    for view in &["aave_proposals", "aave_payloads", "aave_votes"] {
        let rows = storage
            .send_raw_query(&format!("SELECT COUNT(*) AS n FROM {view}"))
            .await
            .unwrap();
        assert_eq!(
            rows.as_array().unwrap()[0]["n"].as_u64().unwrap(),
            0,
            "{view} must be empty"
        );
    }
}

#[tokio::test]
async fn proposals_view_derives_correct_state() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();
    storage
        .run_setup_sql(&[PROPOSALS_VIEW_SQL.to_string()])
        .await
        .unwrap();

    // Proposal 1: full happy path — Created → VotingActivated → Queued → Executed
    // Proposal 2: Created → VotingActivated → Failed
    // Proposal 3: Created → Canceled
    // Proposal 4: Created only
    let logs: Vec<Log> = vec![
        proposal_created_log(1, 17_600_000, 0),
        voting_activated_log(1, 17_600_100, 1),
        proposal_queued_log(1, 500_000, 50_000, 17_600_200, 2),
        proposal_executed_log(1, 17_600_300, 3),
        proposal_created_log(2, 17_700_000, 0),
        voting_activated_log(2, 17_700_100, 1),
        proposal_failed_log(2, 10_000, 800_000, 17_700_200, 2),
        proposal_created_log(3, 17_800_000, 0),
        proposal_canceled_log(3, 17_800_100, 1),
        proposal_created_log(4, 17_900_000, 0),
    ];
    storage.add_events(CHAIN_ID, &logs).await.unwrap();

    let rows = storage
        .send_raw_query(
            r#"SELECT "proposalId", state, "stateId" FROM aave_proposals ORDER BY "proposalId""#,
        )
        .await
        .unwrap();

    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 4);

    assert_eq!(rows[0]["proposalId"], "1");
    assert_eq!(rows[0]["state"], "Executed");
    assert_eq!(rows[0]["stateId"].as_u64().unwrap(), 4);

    assert_eq!(rows[1]["proposalId"], "2");
    assert_eq!(rows[1]["state"], "Failed");
    assert_eq!(rows[1]["stateId"].as_u64().unwrap(), 3);

    assert_eq!(rows[2]["proposalId"], "3");
    assert_eq!(rows[2]["state"], "Canceled");
    assert_eq!(rows[2]["stateId"].as_u64().unwrap(), 5);

    assert_eq!(rows[3]["proposalId"], "4");
    assert_eq!(rows[3]["state"], "Created");
    assert_eq!(rows[3]["stateId"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn proposals_view_includes_timestamp_columns() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();
    storage
        .run_setup_sql(&[PROPOSALS_VIEW_SQL.to_string()])
        .await
        .unwrap();

    let logs: Vec<Log> = vec![
        proposal_created_log(1, 17_600_000, 0),
        voting_activated_log(1, 17_600_100, 1),
        proposal_queued_log(1, 500_000, 50_000, 17_600_200, 2),
        proposal_executed_log(1, 17_600_300, 3),
    ];
    storage.add_events(CHAIN_ID, &logs).await.unwrap();

    let rows = storage
        .send_raw_query(
            r#"SELECT "proposalId", "createdAt", "votingActivatedAt", "executedAt", "canceledAt", "closedAt" FROM aave_proposals"#,
        )
        .await
        .unwrap();

    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "1");
    // Test logs carry no blockTimestamp — the indexer stores 0 as the default.
    assert_eq!(row["createdAt"].as_u64().unwrap(), 0);
    assert_eq!(row["votingActivatedAt"].as_u64().unwrap(), 0);
    assert_eq!(row["executedAt"].as_u64().unwrap(), 0);
    // closedAt comes from b_pq (the queued event); canceledAt is NULL since not canceled.
    assert_eq!(row["closedAt"].as_u64().unwrap(), 0);
    assert!(row["canceledAt"].is_null());
}

#[tokio::test]
async fn payloads_view_returns_payload_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();
    storage
        .run_setup_sql(&[PAYLOADS_VIEW_SQL.to_string()])
        .await
        .unwrap();

    let logs: Vec<Log> = vec![
        payload_sent_log(10, 0, 17_600_000, 0),
        payload_sent_log(10, 1, 17_600_001, 1),
    ];
    storage.add_events(CHAIN_ID, &logs).await.unwrap();

    let rows = storage
        .send_raw_query(
            r#"SELECT "proposalId", "payloadId", "chainId", "sentAt" FROM aave_payloads ORDER BY "payloadId""#,
        )
        .await
        .unwrap();

    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["proposalId"], "10");
    assert_eq!(arr[0]["payloadId"], "0");
    assert_eq!(arr[1]["payloadId"], "1");
    assert_eq!(arr[0]["sentAt"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn votes_view_returns_vote_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();
    storage
        .run_setup_sql(&[VOTES_VIEW_SQL.to_string()])
        .await
        .unwrap();

    let logs: Vec<Log> = vec![
        vote_forwarded_log(5, true, 17_600_000, 0),
        vote_forwarded_log(5, false, 17_600_001, 1),
    ];
    storage.add_events(CHAIN_ID, &logs).await.unwrap();

    let rows = storage
        .send_raw_query(
            r#"SELECT "proposalId", "voter", "support", "votedAt" FROM aave_votes ORDER BY block_number"#,
        )
        .await
        .unwrap();

    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["proposalId"], "5");
    assert_eq!(arr[0]["support"], "true");
    assert_eq!(arr[1]["support"], "false");
    assert_eq!(arr[0]["votedAt"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn vote_emitted_log_is_stored_with_correct_fields() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &[vote_emitted_event()])
        .await
        .unwrap();
    storage
        .add_events(
            CHAIN_ID,
            &[vote_emitted_log(
                42,
                true,
                500_000_000_000_000_000_000_000u128,
                17_500_050,
                0,
            )],
        )
        .await
        .unwrap();

    assert_eq!(count_stored(&storage, &vote_emitted_event()).await, 1);

    let table = event_table_name(CHAIN_ID, &vote_emitted_event());
    let rows = storage
        .send_raw_query(&format!(
            r#"SELECT "proposalId", "voter", "support", "votingPower" FROM {table}"#
        ))
        .await
        .unwrap();
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["proposalId"], "42");
    assert_eq!(
        row["voter"].as_str().unwrap().to_lowercase(),
        "0xcafe000000000000000000000000000000000001"
    );
    assert_eq!(row["support"], "true");
    assert_eq!(row["votingPower"], "500000000000000000000000");
}

#[tokio::test]
async fn full_governance_lifecycle_is_stored() {
    let storage = DuckDBStorage::with_db(":memory:").unwrap();
    storage
        .include_events(CHAIN_ID, &all_governance_events())
        .await
        .unwrap();

    let logs: Vec<Log> = vec![
        proposal_created_log(1, 17_600_000, 0),
        voting_activated_log(1, 17_600_100, 1),
        proposal_queued_log(1, 500_000, 50_000, 17_600_200, 2),
        payload_sent_log(1, 0, 17_600_300, 3),
        proposal_executed_log(1, 17_600_400, 4),
        vote_forwarded_log(1, true, 17_600_050, 5),
        vote_forwarded_log(1, false, 17_600_051, 6),
        proposal_created_log(2, 17_700_000, 0),
        voting_activated_log(2, 17_700_100, 1),
        proposal_failed_log(2, 10_000, 800_000, 17_700_200, 2),
        proposal_created_log(3, 17_800_000, 0),
        proposal_canceled_log(3, 17_800_100, 1),
    ];

    storage.add_events(CHAIN_ID, &logs).await.unwrap();

    assert_eq!(count_stored(&storage, &proposal_created_event()).await, 3);
    assert_eq!(count_stored(&storage, &voting_activated_event()).await, 2);
    assert_eq!(count_stored(&storage, &proposal_queued_event()).await, 1);
    assert_eq!(count_stored(&storage, &proposal_failed_event()).await, 1);
    assert_eq!(count_stored(&storage, &proposal_executed_event()).await, 1);
    assert_eq!(count_stored(&storage, &proposal_canceled_event()).await, 1);
    assert_eq!(count_stored(&storage, &payload_sent_event()).await, 1);
    assert_eq!(count_stored(&storage, &vote_forwarded_event()).await, 2);
}

// ── GraphQL layer tests ───────────────────────────────────────────────────────

fn graphql_field_name(event: &Event) -> String {
    let selector = event.selector().to_string();
    let short_hash = &selector[2..7];
    format!(
        "event_{}_{}_{}",
        CHAIN_ID,
        event.name.to_ascii_lowercase(),
        short_hash
    )
}

#[tokio::test]
async fn graphql_condition_filters_vote_emitted_by_proposal() {
    let tmp = TempDb::new("gql_condition_vote");
    let logs = vec![
        vote_emitted_log(10, true, 1_000_000, 17_600_000, 0),
        vote_emitted_log(10, false, 2_000_000, 17_600_001, 1),
        vote_emitted_log(99, true, 3_000_000, 17_600_002, 2), // different proposal — must be excluded
    ];
    let factory = seed_governance_db(tmp.path(), &[vote_emitted_event()], &logs).await;
    let schema = build_schema_from_factory(factory)
        .await
        .expect("build schema");

    let fname = graphql_field_name(&vote_emitted_event());
    let query = format!(
        r#"{{ {}(condition: {{ proposalId: "10" }}) {{ proposalId voter support votingPower }} }}"#,
        fname
    );
    let res = schema.execute(&query).await;
    assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
    let data = res.data.into_json().unwrap();
    let rows = data[&fname].as_array().expect("expected array");
    assert_eq!(rows.len(), 2, "only votes for proposal 10");
    for row in rows {
        assert_eq!(row["proposalId"].as_str().unwrap(), "10");
    }
}

#[tokio::test]
async fn graphql_where_filters_vote_emitted_by_block_range() {
    let tmp = TempDb::new("gql_where_block");
    let logs = vec![
        vote_emitted_log(5, true, 1_000_000, 17_600_000, 0),
        vote_emitted_log(5, true, 2_000_000, 17_600_100, 1),
        vote_emitted_log(5, true, 3_000_000, 17_600_200, 2),
    ];
    let factory = seed_governance_db(tmp.path(), &[vote_emitted_event()], &logs).await;
    let schema = build_schema_from_factory(factory)
        .await
        .expect("build schema");

    let fname = graphql_field_name(&vote_emitted_event());
    // Only the last two blocks (>= 17_600_100) should be returned.
    let query = format!(
        r#"{{ {}(where: {{ blockNumber: {{ gte: "17600100" }} }}) {{ blockNumber proposalId }} }}"#,
        fname
    );
    let res = schema.execute(&query).await;
    assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
    let data = res.data.into_json().unwrap();
    let rows = data[&fname].as_array().expect("expected array");
    assert_eq!(rows.len(), 2);
    for row in rows {
        let bn: u64 = row["blockNumber"].as_str().unwrap().parse().unwrap();
        assert!(bn >= 17_600_100);
    }
}

#[tokio::test]
async fn graphql_condition_and_where_combine_on_governance_events() {
    let tmp = TempDb::new("gql_combined");
    let logs = vec![
        vote_emitted_log(42, true, 1_000_000, 17_600_000, 0),
        vote_emitted_log(42, false, 2_000_000, 17_600_050, 1),
        vote_emitted_log(42, true, 3_000_000, 17_600_100, 2),
        vote_emitted_log(99, true, 4_000_000, 17_600_100, 3), // different proposal
    ];
    let factory = seed_governance_db(tmp.path(), &[vote_emitted_event()], &logs).await;
    let schema = build_schema_from_factory(factory)
        .await
        .expect("build schema");

    let fname = graphql_field_name(&vote_emitted_event());
    // condition pins proposalId=42, where restricts to blockNumber >= 17_600_050 → 2 rows.
    let query = format!(
        r#"{{ {}(condition: {{ proposalId: "42" }}, where: {{ blockNumber: {{ gte: "17600050" }} }}) {{ proposalId blockNumber }} }}"#,
        fname
    );
    let res = schema.execute(&query).await;
    assert!(res.errors.is_empty(), "errors: {:?}", res.errors);
    let data = res.data.into_json().unwrap();
    let rows = data[&fname].as_array().expect("expected array");
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row["proposalId"].as_str().unwrap(), "42");
    }
}
