// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Module with utilities for testing.

use alloy::{json_abi::Event, rpc::types::Log};
use fake::{Fake, Faker};
use std::str::FromStr;

/// Generates a random Ethereum address (20 bytes, 0x-prefixed)
pub fn fake_address() -> String {
    let bytes: [u8; 20] = Faker.fake();
    format!("0x{}", hex::encode(bytes))
}

/// Converts an Ethereum address to a topic.
pub fn address_to_topic(address: &str) -> String {
    let trimmed = address.trim_start_matches("0x");
    format!("0x{:0>64}", trimmed)
}

/// Generates a random uint256 hex value.
pub fn random_uint256_hex() -> String {
    let bytes: [u8; 32] = Faker.fake();
    format!("0x{}", hex::encode(bytes))
}

/// Picks a random address from a pool.
pub fn pick_pool_address(pool: &[String]) -> &str {
    let index = (Faker.fake::<u32>() as usize) % pool.len();
    pool[index].as_str()
}

/// ERC20 Transfer event
pub fn transfer_event() -> Event {
    Event::from_str("event Transfer(address indexed from, address indexed to, uint256 value)")
        .expect("failed to parse Transfer event")
}

/// ERC20 Approval event
pub fn approval_event() -> Event {
    Event::from_str("event Approval(address indexed owner, address indexed spender, uint256 value)")
        .expect("failed to parse Approval event")
}

/// Mint event (common in many token contracts)
pub fn mint_event() -> Event {
    Event::from_str("event Mint(address indexed to, uint256 amount)")
        .expect("failed to parse Mint event")
}

/// Burn event
pub fn burn_event() -> Event {
    Event::from_str("event Burn(address indexed from, uint256 amount)")
        .expect("failed to parse Burn event")
}

/// ERC721 Transfer event
pub fn erc721_transfer_event() -> Event {
    Event::from_str(
        "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
    )
    .expect("failed to parse ERC721 Transfer event")
}

/// Collection of standard ERC20 events
pub fn erc20_events() -> Vec<Event> {
    vec![transfer_event(), approval_event()]
}

/// Collection of token events including mint/burn
pub fn token_events() -> Vec<Event> {
    vec![
        transfer_event(),
        approval_event(),
        mint_event(),
        burn_event(),
    ]
}

/// Enum that represents the kind of event for a log.
#[derive(Copy, Clone)]
pub enum LogEventKind {
    Erc20Transfer,
    Erc20Approval,
    Erc721Transfer,
}

/// Fixture for generating logs.
///
/// This fixture is used to generate logs for testing. It can be configured to generate logs for different
/// event kinds and contract addresses.
pub struct LogTestFixture {
    log_count: usize,
    start_block: u64,
    event_kinds: Vec<LogEventKind>,
    contract_addresses: Vec<String>,
    address_pool: Option<Vec<String>>,
    address_pool_size: usize,
}

impl LogTestFixture {
    pub fn builder() -> Self {
        // Generate a random start block between 1 and 1_000_000
        let start_block: u64 = (Faker.fake::<u32>() % 1_000_000 + 1) as u64;
        Self {
            log_count: 1,
            start_block,
            event_kinds: Vec::new(),
            contract_addresses: Vec::new(),
            address_pool: None,
            address_pool_size: 5,
        }
    }

    pub fn with_log_count(mut self, log_count: usize) -> Self {
        self.log_count = log_count;
        self
    }

    pub fn with_start_block(mut self, start_block: u64) -> Self {
        self.start_block = start_block;
        self
    }

    pub fn with_contract_addresses<I>(mut self, contract_addresses: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        self.contract_addresses = contract_addresses.into_iter().collect();
        self
    }

    pub fn add_contract_address<S>(mut self, address: S) -> Self
    where
        S: Into<String>,
    {
        self.contract_addresses.push(address.into());
        self
    }

    pub fn with_address_pool_size(mut self, size: usize) -> Self {
        self.address_pool_size = size;
        self
    }

    pub fn with_address_pool<I>(mut self, addresses: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        self.address_pool = Some(addresses.into_iter().collect());
        self
    }

    pub fn with_erc20_transfer(mut self) -> Self {
        self.event_kinds.push(LogEventKind::Erc20Transfer);
        self
    }

    pub fn with_erc20_approval(mut self) -> Self {
        self.event_kinds.push(LogEventKind::Erc20Approval);
        self
    }

    pub fn with_erc721_transfer(mut self) -> Self {
        self.event_kinds.push(LogEventKind::Erc721Transfer);
        self
    }

    pub fn build(self) -> Vec<Log> {
        let event_kinds = if self.event_kinds.is_empty() {
            vec![LogEventKind::Erc20Transfer]
        } else {
            self.event_kinds
        };

        let contract_addresses = if self.contract_addresses.is_empty() {
            vec![fake_address()]
        } else {
            self.contract_addresses
        };

        let address_pool = match self.address_pool {
            Some(pool) if !pool.is_empty() => pool,
            _ => {
                let pool_size = self.address_pool_size.max(1);
                (0..pool_size).map(|_| fake_address()).collect()
            }
        };

        (0..self.log_count)
            .map(|i| {
                let event_kind = event_kinds[i % event_kinds.len()];
                let contract_address = &contract_addresses[i % contract_addresses.len()];
                let from_address = pick_pool_address(&address_pool);
                let to_address = pick_pool_address(&address_pool);

                let (topics, data) = match event_kind {
                    LogEventKind::Erc20Transfer => (
                        vec![
                            transfer_event().selector().to_string(),
                            address_to_topic(from_address),
                            address_to_topic(to_address),
                        ],
                        random_uint256_hex(),
                    ),
                    LogEventKind::Erc20Approval => (
                        vec![
                            approval_event().selector().to_string(),
                            address_to_topic(from_address),
                            address_to_topic(to_address),
                        ],
                        random_uint256_hex(),
                    ),
                    LogEventKind::Erc721Transfer => (
                        vec![
                            erc721_transfer_event().selector().to_string(),
                            address_to_topic(from_address),
                            address_to_topic(to_address),
                            random_uint256_hex(),
                        ],
                        "0x".to_string(),
                    ),
                };

                let block_number = self.start_block + i as u64;
                serde_json::from_value(serde_json::json!({
                    "address": contract_address,
                    "topics": topics,
                    "data": data,
                    "blockNumber": format!("0x{:x}", block_number),
                    "transactionHash": format!("0x{:064x}", block_number),
                    "transactionIndex": "0x0",
                    "blockHash": format!("0x{:064x}", 0xbabe_u64),
                    "blockTimestamp": format!("0x{:x}", 1700000000 + block_number),
                    "logIndex": format!("0x{:x}", i),
                    "removed": false
                }))
                .expect("failed to build log")
            })
            .collect()
    }
}
