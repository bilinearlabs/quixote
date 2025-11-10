// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event collector.

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::B256;
use alloy::primitives::{Address, keccak256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, FilterSet, Log};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct EventCollector<'a> {
    contract_address: &'a Address,
    events: &'a [String],
    start_block: &'a BlockNumberOrTag,
    end_block: &'a BlockNumberOrTag,
    provider: Arc<dyn Provider + Send + Sync>,
    producer_buffer: Sender<Vec<Log>>,
}

impl<'a> EventCollector<'a> {
    pub fn new(
        contract_address: &'a Address,
        events: &'a [String],
        start_block: &'a BlockNumberOrTag,
        end_block: &'a BlockNumberOrTag,
        provider: Arc<dyn Provider + Send + Sync>,
        producer_buffer: Sender<Vec<Log>>,
    ) -> Self {
        Self {
            contract_address,
            events,
            start_block,
            end_block,
            provider,
            producer_buffer,
        }
    }

    pub async fn collect(&self) -> Result<()> {
        let mut filter_events = FilterSet::default();

        // Hash each event signature and add to the filter
        for event_signature in self.events {
            let hash = keccak256(event_signature.as_bytes());
            // keccak256 returns [u8; 32], which B256 can be created from
            let event_hash = B256::from(hash);
            filter_events.insert(event_hash);
        }

        let events = self
            .provider
            .get_logs(
                &Filter::new()
                    .from_block(*self.start_block)
                    .to_block(*self.end_block)
                    .address(*self.contract_address)
                    .event_signature(filter_events),
            )
            .await?;

        self.producer_buffer.send(events).await?;

        Ok(())
    }
}
