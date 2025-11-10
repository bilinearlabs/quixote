// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event processor.

use crate::Storage;
use alloy::rpc::types::Log;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use anyhow::Result;

pub struct EventProcessor {
    storage: Arc<dyn Storage + Send + Sync>,
    producer_buffer: Receiver<Vec<Log>>,
}

impl EventProcessor {
    pub fn new(storage: Arc<dyn Storage + Send + Sync>, producer_buffer: Receiver<Vec<Log>>) -> Self {
        Self { storage, producer_buffer }
    }

    pub async fn process(&mut self) -> Result<()> {
        while let Some(events) = self.producer_buffer.recv().await {
            self.storage.add_events(&events)?;
        }

        Ok(())
    }
}