// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the connection to the DuckDB database.

const DUCKDB_FILE_PATH: &str = "etherduck_indexer.duckdb";
const DUCKDB_SCHEMA_VERSION: &str = "0.1.0";
const DUCKDB_BASE_TABLE_NAME: &str = "etherduck_info";

use alloy::{
    primitives::{B256, keccak256},
    rpc::types::Log,
};
use anyhow::Result;
use duckdb::Connection;

pub trait Storage {
    fn add_events(&self, events: &[Log]) -> Result<()>;
}

pub struct DuckDBStorage {
    conn: Connection,
}

impl Storage for DuckDBStorage {
    fn add_events(&self, events: &[Log]) -> Result<()> {
        for event in events {
            println!("Adding event: {:?}", event);
        }
        Ok(())
    }
}

impl DuckDBStorage {
    pub fn new() -> Result<DuckDBStorage> {
        println!("Using the default DB");
        Self::with_db(DUCKDB_FILE_PATH)
    }

    /// Creates a new DuckDBStorage with the given database path.
    pub fn with_db(db_path: &str) -> Result<DuckDBStorage> {
        let conn = Connection::open(db_path).expect("failed to open duckdb database");

        let table_exists: bool = conn.query_row(
            r#"
                SELECT
                    count(*)
                FROM
                    information_schema.tables
                WHERE
                    table_schema = 'main'
                    AND table_name = ?
                    AND table_type = 'BASE TABLE';"#,
            [DUCKDB_BASE_TABLE_NAME],
            |row| row.get(0),
        )?;

        if !table_exists {
            println!("Initializing DB...");
            DuckDBStorage::create_db_base(&conn)?;
        } else {
            // Try to retrieve version from etherduck_info table using a query
            let version: String =
                conn.query_row("SELECT version FROM etherduck_info LIMIT 1", [], |row| {
                    row.get(0)
                })?;

            if version != DUCKDB_SCHEMA_VERSION {
                println!("Your database is out of date. Please run the database upgrade.");
            }
        }

        println!("Your database is loaded and ready to use.");

        Ok(DuckDBStorage { conn })
    }

    pub fn include_events(&self, events: &[String]) -> Result<()> {
        for event in events {
            let hash = keccak256(event.as_bytes());
            let event_hash = B256::from(hash).to_string();
            DuckDBStorage::create_event_schema(&self.conn, &event_hash)?;
        }
        Ok(())
    }

    fn create_db_base(conn: &Connection) -> Result<()> {
        let statement = format!(
            "
            BEGIN;
            CREATE TABLE IF NOT EXISTS {DUCKDB_BASE_TABLE_NAME}(
                version VARCHAR NOT NULL, 
                first_block BIGINT, 
                last_block BIGINT,
                PRIMARY KEY (version)
            );
            CREATE TABLE IF NOT EXISTS blocks(
                block_number BIGINT NOT NULL,
                block_hash VARCHAR(42) NOT NULL,
                block_timestamp BIGINT NOT NULL,
                PRIMARY KEY (block_number)
            );
            CREATE TABLE IF NOT EXISTS event_descriptor(
                event_signature VARCHAR(42) NOT NULL,
                event_name VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                PRIMARY KEY (event_signature)
            );
            COMMIT;"
        );
        conn.execute_batch(&statement)?;

        conn.execute(
            &format!(
                "INSERT INTO {} (version, first_block, last_block) 
                VALUES (?, 0, 0);",
                DUCKDB_BASE_TABLE_NAME
            ),
            [DUCKDB_SCHEMA_VERSION],
        )?;

        conn.execute(
            "INSERT INTO event_descriptor (event_signature, event_name, event_type) 
            VALUES (?, ?, ?);",
            [
                B256::from(keccak256(b"Transfer")).to_string(),
                "Transfer".to_owned(),
                "ERC20".to_owned(),
            ],
        )?;

        Ok(())
    }

    fn create_event_schema(conn: &Connection, event_hash: &str) -> Result<()> {
        let statement = &format!(
            "
                CREATE TABLE IF NOT EXISTS event_{event_hash}(
                    block_number BIGINT NOT NULL,
                    transaction_hash VARCHAR(42) NOT NULL,
                    log_index SMALLINT NOT NULL,
                    contract_address VARCHAR(42) NOT NULL,
                    topic0 VARCHAR(42),
                    topic1 VARCHAR(42),
                    topic2 VARCHAR(42),
                    topic3 VARCHAR(42),
                    PRIMARY KEY (block_number, transaction_hash)
                );
            ",
        );

        conn.execute(statement, [])?;

        Ok(())
    }
}
