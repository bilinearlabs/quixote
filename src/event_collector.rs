// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Module for the event collector.

use crate::{
    CollectorSeed, LogChunk, OptionalAddressDisplay, TxLogChunk, constants::*, error_codes,
    metrics::MetricsHandle,
};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::Provider,
    rpc::types::{Filter, SyncStatus},
};
use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, instrument, warn};

#[derive(Clone)]
pub struct EventCollector {
    chain_id: u64,
    contract_address: Option<Address>,
    filter: Option<Filter>,
    start_block: u64,
    provider: Arc<dyn Provider + Send + Sync>,
    sync_mode: BlockNumberOrTag,
    poll_interval: u64,
    producer_buffer: TxLogChunk,
    default_block_range: usize,
    block_range_hint_regex: regex::Regex,
    metrics: MetricsHandle,
}

impl EventCollector {
    pub fn new(
        provider: Arc<dyn Provider + Send + Sync>,
        producer_buffer: TxLogChunk,
        seed: &CollectorSeed,
        metrics: MetricsHandle,
    ) -> Self {
        // Regex to capture the last two integers (block numbers) from messages like:
        // "error code -32602: query exceeds max results 20000, retry with the range 22382105-22382515"
        let block_range_hint_regex = regex::Regex::new(r"(\d+)-(\d+)\s*$").unwrap();

        Self {
            chain_id: seed.chain_id,
            contract_address: seed.contract_address,
            filter: seed.filter.clone(),
            start_block: seed.start_block,
            provider,
            sync_mode: seed.sync_mode,
            poll_interval: DEFAULT_POLL_INTERVAL,
            producer_buffer,
            default_block_range: seed.block_range,
            block_range_hint_regex,
            metrics,
        }
    }

    #[instrument(
        name = "event_collector",
        skip(self),
        fields(
            chain_id = %self.chain_id,
            contract_address = %self.contract_address.display_or_none()
        )
    )]
    pub async fn collect(&self) -> Result<()> {
        if self.check_sync_status().await? {
            error!(
                "The RPC server is syncing, resume indexing when the syncing process is complete"
            );
            std::process::exit(error_codes::ERROR_CODE_RPC_SERVER_SYNCING);
        }

        // The last stored block number in the DB.
        let mut processed_to = self.start_block.saturating_sub(1);
        // How many blocks per get_logs call.
        let mut chunk_length = self.default_block_range as u64;
        // Counts how many successful chunks have been processed.
        let mut successful_counter: u8 = 0;
        // Flag that indicates whether we are backfilling the database or fetching the finalized block.
        let mut backfill_mode = true;

        loop {
            // Check tha the RPC server is not syncing. If syncing, a raw exit is issued as we prefer to stop
            // all the running logic just in case some RPC request returns inconsistent data that gets
            // stored in the database.
            if self.check_sync_status().await? {
                error!(
                    "The RPC server is syncing, resume indexing when the syncing process is complete"
                );
                std::process::exit(error_codes::ERROR_CODE_RPC_SERVER_SYNCING);
            }

            let provider = self.provider.clone();
            let finalized_block = match provider.get_block_by_number(self.sync_mode).await {
                Ok(Some(block)) => block.header.number,
                Ok(None) => return Err(anyhow::anyhow!("Finalized block is None")),
                Err(e) => {
                    error!("Failed to get finalized block from RPC provider: {}", e);
                    return Err(anyhow::anyhow!("RPC connection error: {}", e));
                }
            };

            // Export the chain head so consumers can compare with the indexed progress.
            self.metrics.record_chain_head_block(
                self.chain_id,
                &self.contract_address.display_or_none(),
                finalized_block,
            );

            let remaining = finalized_block.saturating_sub(processed_to);
            if remaining == 0 {
                // First time, we completed the backfill of the DB, let's inform the user.
                if backfill_mode {
                    info!(
                        "Backfill of the database completed, starting to fetch the finalized block."
                    );
                }
                backfill_mode = false;
                sleep(Duration::from_secs(self.poll_interval)).await;
                continue;
            }

            // Prepare a run of up to 5 times the chunk size.
            let (chunk_starts, step) = if remaining > CHUNK_MULTIPLIER * chunk_length {
                (
                    ((processed_to + 1)..=processed_to + CHUNK_MULTIPLIER * chunk_length)
                        .step_by(chunk_length as usize)
                        .collect::<Vec<u64>>(),
                    processed_to + CHUNK_MULTIPLIER * chunk_length,
                )
            } else {
                (
                    ((processed_to + 1)..=finalized_block)
                        .step_by(chunk_length as usize)
                        .collect::<Vec<u64>>(),
                    finalized_block,
                )
            };

            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let provider_clone = self.provider.clone();

            // Process the prepared chunks of the chain history concurrently.
            // If an error is detected in the series, all the intermediate results are discarded, and the logic
            // proceeds to reduce the block range and retry the series.
            let rpc_results: Vec<LogChunk> = match stream::iter(chunk_starts.into_iter())
                .map(|chunk_start| {
                    let provider = provider_clone.clone();

                    async move {
                        let chunk_end =
                            std::cmp::min(chunk_start + chunk_length - 1, finalized_block);

                        info!("Fetching events for blocks [{chunk_start}-{chunk_end}]");

                        // Build the base filter for the get_Logs call. By default, all the events for a given smart
                        // contract are fetched.
                        let filter = Filter::new().from_block(chunk_start).to_block(chunk_end);

                        let mut filter = match contract_address {
                            Some(contract_address) => filter.address(contract_address),
                            None => filter,
                        };

                        // Add custom filters (topics) if provided.
                        if let Some(custom_filter) = self.filter.clone()
                            && !custom_filter.topics.is_empty()
                        {
                            filter.topics = custom_filter.topics;
                        }

                        let events = provider.get_logs(&filter).await?;

                        debug!(
                            "Fetched {} events for blocks [{chunk_start}-{chunk_end}]",
                            events.len()
                        );

                        Ok::<_, anyhow::Error>(LogChunk {
                            start_block: chunk_start,
                            end_block: chunk_end,
                            events,
                        })
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_RPC_REQUESTS)
                .try_collect()
                .await
            {
                Ok(results) => results,
                Err(e) => {
                    let err_msg = e.to_string();
                    debug!("Discarded block range: [{processed_to}-{step}]");

                    if err_msg.contains("-32602") {
                        // Did we receive a hint of the valid range?
                        let prev_chunk_length = chunk_length;
                        chunk_length = if let Some(captures) =
                            self.block_range_hint_regex.captures(&err_msg)
                        {
                            // Extract the two numbers from the regex capture groups
                            if let (Some(first_match), Some(second_match)) =
                                (captures.get(1), captures.get(2))
                            {
                                let first: u64 = first_match.as_str().parse().unwrap_or(0);
                                let second: u64 = second_match.as_str().parse().unwrap_or(0);
                                let range = second.saturating_sub(first);
                                // And apply a safety margin of a 10% of the range.
                                range - (range / 10)
                            } else {
                                chunk_length >> 1
                            }
                        } else {
                            chunk_length >> 1
                        };

                        if chunk_length == 0 {
                            return Err(anyhow::anyhow!(
                                "Block range reduced to zero, cannot continue the indexing."
                            ));
                        }

                        warn!(
                            "Throttled RPC server, reducing block range from {prev_chunk_length} to {chunk_length}"
                        );

                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Error received from the RPC server: {e}"));
                    }
                }
            };

            // Reached this statement, we are sure that no errors happened in the series of RPC requests, thus we
            // can send the results to the consumer task for its storage in the DB.
            // Results are sent sequentially to preserve block order for the receiver.
            for result in rpc_results {
                producer_buffer.send(result).await?;
            }

            // If we reach the threshold of successful chunks, we can restore the block range to the default value.
            if successful_counter == SUCCESSFUL_CHUNKS_THRESHOLD {
                chunk_length = self.default_block_range as u64;
            }

            processed_to = step;
            successful_counter = successful_counter.wrapping_add(1);
        }
    }

    async fn check_sync_status(&self) -> Result<bool> {
        let syncing = self.provider.syncing().await?;

        match syncing {
            SyncStatus::Info(_) => Ok(true),
            SyncStatus::None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the EventCollector.
    //!
    //! # Description
    //!
    //! The tests included in this module are meant to ensure that the fetching logic behaves as expected. Single
    //! [EventCollector] instances are launched for fetching some known block ranges whose results are compared
    //! against a known set of events.

    use crate::metrics::MetricsConfig;

    use super::*;
    use alloy::{
        json_abi::{Event, JsonAbi},
        primitives::{B256, address},
        providers::{Provider, ProviderBuilder},
        rpc::client::RpcClient,
        transports::http::reqwest::Url,
    };
    use pretty_assertions::assert_eq;
    use rstest::*;
    use std::{str::FromStr, sync::Arc};
    use tokio::sync::mpsc;

    const TRANSFER_EVENT_HASH: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    // Test constants for block 24022000 filter tests (verified via eth_getLogs RPC call)
    const TEST_BLOCK: u64 = 24022000;
    /// Total events in block 24022000 for USDC contract (100 Transfer + 8 Approval = 108)
    const EVENTS_IN_BLOCK: usize = 108;
    /// Address that appears as "from" (topic1) in 8 events
    const TEST_FROM_ADDRESS: &str = "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43";
    const TEST_FROM_ADDRESS_COUNT: usize = 8;
    /// Address that appears as "from" (topic1) in 6 events
    const TEST_FROM_ADDRESS_2: &str = "0x42e213a3ad048e899b89ea8cb11d21bc97b84748";
    const TEST_FROM_ADDRESS_2_COUNT: usize = 6;
    /// Address that appears as "to" (topic2) for one of TEST_FROM_ADDRESS transfers
    /// Note: original had typo "aed" instead of "aeed" - verified via RPC
    const TEST_TO_ADDRESS: &str = "0x22f24bb0279fe1ca9aeed0b7d370dccea32c0e87";
    /// How many transfer events are expected to be fetched within the block range 24022000-24023000
    /// for the USDC contract.
    const TRANSFER_EVENT_COUNT: usize = 29911;

    /// Target block for the short test.
    const TARGET_BLOCK_SHORT_TEST: u64 = 24022500;

    /// Builds RPC URL from environment variables using standard URL format.
    /// Format: http://user:pass@host:port
    #[fixture]
    fn rpc_url_fixture() -> Url {
        use std::env;

        // Get credentials and URL from environment variables
        let rpc_url = env::var("QUIXOTE_TEST_RPC").expect("QUIXOTE_TEST_RPC must be set");
        let rpc_user =
            env::var("QUIXOTE_TEST_RPC_USER").expect("QUIXOTE_TEST_RPC_USER must be set");
        let rpc_password =
            env::var("QUIXOTE_TEST_RPC_PASSWORD").expect("QUIXOTE_TEST_RPC_PASSWORD must be set");

        // Parse the base URL and inject credentials
        let mut url = Url::parse(&rpc_url).expect("Failed to parse QUIXOTE_TEST_RPC as URL");
        url.set_username(&rpc_user).expect("Failed to set username");
        url.set_password(Some(&rpc_password))
            .expect("Failed to set password");
        url
    }

    #[fixture]
    fn provider_fixture(rpc_url_fixture: Url) -> Arc<dyn Provider + Send + Sync + 'static> {
        Arc::new(ProviderBuilder::new().connect_client(RpcClient::builder().http(rpc_url_fixture)))
    }

    /// Chain ID for Ethereum mainnet (used in integration tests).
    const TEST_CHAIN_ID: u64 = 1;

    #[fixture]
    fn seed_fixture(usdc_events_fixture: Vec<Event>, rpc_url_fixture: Url) -> CollectorSeed {
        CollectorSeed {
            chain_id: TEST_CHAIN_ID,
            rpc_url: rpc_url_fixture,
            // USDC contract address
            contract_address: Some(address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")),
            events: usdc_events_fixture,
            start_block: 24022000,
            sync_mode: BlockNumberOrTag::Latest,
            filter: None,
            block_range: crate::constants::DEFAULT_BLOCK_RANGE,
        }
    }

    #[fixture]
    fn usdc_events_fixture() -> Vec<Event> {
        let json = std::fs::read_to_string("./test/fixtures/usdc_abi.json")
            .expect("Failed to read USDC ABI file");
        let abi: JsonAbi = serde_json::from_str(&json).expect("Failed to parse USDC ABI");
        let events = abi.events().cloned().collect::<Vec<Event>>();
        events
    }

    /// This test ensures that the fetcher retrieves the expected amount of transfer events for a given block range.
    #[rstest]
    #[tokio::test]
    async fn check_transfer_events_for_a_block_range(
        provider_fixture: Arc<dyn Provider + Send + Sync + 'static>,
        mut seed_fixture: CollectorSeed,
    ) {
        let (producer_buffer, mut consumer_buffer) = mpsc::channel(1000);
        let metrics = crate::metrics::MetricsHandle::default();
        // A block range of 10 blocks is the safest choice to avoid throttling the RPC server.
        seed_fixture.block_range = 10;
        let mut collector =
            EventCollector::new(provider_fixture, producer_buffer, &seed_fixture, metrics);
        collector.sync_mode = BlockNumberOrTag::Number(TARGET_BLOCK_SHORT_TEST);

        let handle = tokio::spawn(async move {
            collector.collect().await.unwrap();
        });

        // Give enough time to fetch the events.
        sleep(Duration::from_secs(20)).await;

        handle.abort();

        let mut transfer_events = 0;

        while let Some(result) = consumer_buffer.recv().await {
            transfer_events += result
                .events
                .iter()
                .filter(|log| log.topic0().unwrap().to_string() == TRANSFER_EVENT_HASH)
                .count();
        }

        assert_eq!(
            transfer_events, TRANSFER_EVENT_COUNT,
            "The number of transfer events fetched is not the expected one"
        );
    }

    /// This test ensures that the fetcher handles an error coming from the RPC server due to throttling.
    #[rstest]
    #[tokio::test]
    async fn simple_throttling_test(
        provider_fixture: Arc<dyn Provider + Send + Sync + 'static>,
        mut seed_fixture: CollectorSeed,
    ) {
        let (producer_buffer, mut consumer_buffer) = mpsc::channel(1000);
        let metrics = crate::metrics::MetricsHandle::default();
        // 10k throttles the RPC at the second request.
        seed_fixture.block_range = 10000;
        let mut collector =
            EventCollector::new(provider_fixture, producer_buffer, &seed_fixture, metrics);
        collector.sync_mode = BlockNumberOrTag::Number(TARGET_BLOCK_SHORT_TEST);

        let handle = tokio::spawn(async move {
            collector.collect().await.unwrap();
        });

        // Give enough time to fetch the events.
        sleep(Duration::from_secs(40)).await;

        handle.abort();

        let mut transfer_events = 0;

        while let Some(result) = consumer_buffer.recv().await {
            transfer_events += result
                .events
                .iter()
                .filter(|log| log.topic0().unwrap().to_string() == TRANSFER_EVENT_HASH)
                .count();
        }

        assert_eq!(
            transfer_events, TRANSFER_EVENT_COUNT,
            "The number of transfer events fetched is not the expected one"
        );
    }

    /// Fixture for Transfer event only (no full ABI).
    #[fixture]
    fn transfer_event_fixture() -> Vec<Event> {
        vec![
            Event::parse("Transfer(address indexed from, address indexed to, uint256 value)")
                .expect("Failed to parse Transfer event"),
        ]
    }

    /// Seed fixture for block 24022000 without filters.
    #[fixture]
    fn seed_single_block_no_filter(
        transfer_event_fixture: Vec<Event>,
        rpc_url_fixture: Url,
    ) -> CollectorSeed {
        CollectorSeed {
            chain_id: TEST_CHAIN_ID,
            rpc_url: rpc_url_fixture,
            contract_address: Some(address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")),
            events: transfer_event_fixture,
            start_block: TEST_BLOCK,
            sync_mode: BlockNumberOrTag::Number(TEST_BLOCK),
            filter: None,
            block_range: 10,
        }
    }

    /// Helper to create a filter with topic1 (from) filter values.
    fn create_from_filter(addresses: Vec<&str>) -> Filter {
        let topic_values: Vec<B256> = addresses
            .iter()
            .map(|addr| {
                let hex = addr.strip_prefix("0x").unwrap_or(addr);
                let padded = format!("{:0>64}", hex);
                B256::from_str(&padded).unwrap()
            })
            .collect();

        let mut filter = Filter::new();
        filter.topics[1] = topic_values.into();
        filter
    }

    /// Helper to create a filter with topic1 (from) and topic2 (to) filter values.
    fn create_from_and_to_filter(from_addresses: Vec<&str>, to_addresses: Vec<&str>) -> Filter {
        let from_values: Vec<B256> = from_addresses
            .iter()
            .map(|addr| {
                let hex = addr.strip_prefix("0x").unwrap_or(addr);
                let padded = format!("{:0>64}", hex);
                B256::from_str(&padded).unwrap()
            })
            .collect();

        let to_values: Vec<B256> = to_addresses
            .iter()
            .map(|addr| {
                let hex = addr.strip_prefix("0x").unwrap_or(addr);
                let padded = format!("{:0>64}", hex);
                B256::from_str(&padded).unwrap()
            })
            .collect();

        let mut filter = Filter::new();
        filter.topics[1] = from_values.into();
        filter.topics[2] = to_values.into();
        filter
    }

    /// Helper to run a collector for a single block and count events.
    async fn run_collector_and_count(
        provider: Arc<dyn Provider + Send + Sync + 'static>,
        mut seed: CollectorSeed,
    ) -> usize {
        let (producer_buffer, mut consumer_buffer) = mpsc::channel(1000);
        seed.block_range = 10;

        let metrics = MetricsHandle::new(&MetricsConfig {
            enabled: false,
            address: "127.0.0.1".to_string(),
            port: 5054,
            allow_origin: None,
        })
        .unwrap();
        let collector = EventCollector::new(provider, producer_buffer, &seed, metrics);

        let handle = tokio::spawn(async move {
            collector.collect().await.unwrap();
        });

        // Give enough time to fetch the single block.
        sleep(Duration::from_secs(5)).await;
        handle.abort();

        let mut event_count = 0;
        while let Some(result) = consumer_buffer.recv().await {
            event_count += result.events.len();
        }
        event_count
    }

    /// Test: Index block 24022000 without filters.
    /// Expected: All 91 Transfer events for USDC in that block.
    #[rstest]
    #[tokio::test]
    async fn filter_test_no_filter_gets_all_events(
        provider_fixture: Arc<dyn Provider + Send + Sync + 'static>,
        seed_single_block_no_filter: CollectorSeed,
    ) {
        let event_count =
            run_collector_and_count(provider_fixture, seed_single_block_no_filter).await;

        assert_eq!(
            event_count, EVENTS_IN_BLOCK,
            "Expected {} events without filter, got {}",
            EVENTS_IN_BLOCK, event_count
        );
    }

    /// Test: Filter by single "from" address.
    /// Expected: 8 Transfer events from 0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43.
    #[rstest]
    #[tokio::test]
    async fn filter_test_single_from_address(
        provider_fixture: Arc<dyn Provider + Send + Sync + 'static>,
        transfer_event_fixture: Vec<Event>,
        rpc_url_fixture: Url,
    ) {
        let seed = CollectorSeed {
            chain_id: TEST_CHAIN_ID,
            rpc_url: rpc_url_fixture,
            contract_address: Some(address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")),
            events: transfer_event_fixture,
            start_block: TEST_BLOCK,
            sync_mode: BlockNumberOrTag::Number(TEST_BLOCK),
            filter: Some(create_from_filter(vec![TEST_FROM_ADDRESS])),
            block_range: 10,
        };

        let event_count = run_collector_and_count(provider_fixture, seed).await;

        assert_eq!(
            event_count, TEST_FROM_ADDRESS_COUNT,
            "Expected {} Transfer events from {}, got {}",
            TEST_FROM_ADDRESS_COUNT, TEST_FROM_ADDRESS, event_count
        );
    }

    /// Test: Filter by "from" AND "to" addresses.
    /// Expected: 1 Transfer event (from 0xa9d1...3e43 to 0x22f2...e87).
    #[rstest]
    #[tokio::test]
    async fn filter_test_from_and_to_addresses(
        provider_fixture: Arc<dyn Provider + Send + Sync + 'static>,
        transfer_event_fixture: Vec<Event>,
        rpc_url_fixture: Url,
    ) {
        let seed = CollectorSeed {
            chain_id: TEST_CHAIN_ID,
            rpc_url: rpc_url_fixture,
            contract_address: Some(address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")),
            events: transfer_event_fixture,
            start_block: TEST_BLOCK,
            sync_mode: BlockNumberOrTag::Number(TEST_BLOCK),
            filter: Some(create_from_and_to_filter(
                vec![TEST_FROM_ADDRESS],
                vec![TEST_TO_ADDRESS],
            )),
            block_range: 10,
        };

        let event_count = run_collector_and_count(provider_fixture, seed).await;

        assert_eq!(
            event_count, 1,
            "Expected 1 Transfer event with from={} AND to={}, got {}",
            TEST_FROM_ADDRESS, TEST_TO_ADDRESS, event_count
        );
    }

    /// Test: Filter by multiple "from" addresses (OR logic).
    /// Expected: 8 + 3 = 11 Transfer events from either address.
    #[rstest]
    #[tokio::test]
    async fn filter_test_multiple_from_addresses_or_logic(
        provider_fixture: Arc<dyn Provider + Send + Sync + 'static>,
        transfer_event_fixture: Vec<Event>,
        rpc_url_fixture: Url,
    ) {
        let seed = CollectorSeed {
            chain_id: TEST_CHAIN_ID,
            rpc_url: rpc_url_fixture,
            contract_address: Some(address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")),
            events: transfer_event_fixture,
            start_block: TEST_BLOCK,
            sync_mode: BlockNumberOrTag::Number(TEST_BLOCK),
            filter: Some(create_from_filter(vec![
                TEST_FROM_ADDRESS,
                TEST_FROM_ADDRESS_2,
            ])),
            block_range: 10,
        };

        let event_count = run_collector_and_count(provider_fixture, seed).await;

        let expected = TEST_FROM_ADDRESS_COUNT + TEST_FROM_ADDRESS_2_COUNT;
        assert_eq!(
            event_count, expected,
            "Expected {} Transfer events from {} OR {}, got {}",
            expected, TEST_FROM_ADDRESS, TEST_FROM_ADDRESS_2, event_count
        );
    }
}
