// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that includes logic to parse a JSON with the definition of a smartcontract's ABI.

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AbiParams {
    pub anonymous: bool,
    pub inputs: Vec<AbiParam>,
    pub name: String,
    #[serde(rename = "type")]
    pub param_type: String,
}

#[derive(Debug, Deserialize)]
pub struct AbiParam {
    pub indexed: bool,
    #[serde(rename = "internalType")]
    pub internal_type: String,
    pub name: String,
    #[serde(rename = "type")]
    pub param_type: String,
}

#[derive(Debug, Clone)]
pub struct AbiParser {
    abi: serde_json::Value,
}

impl AbiParser {
    pub fn new(abi: serde_json::Value) -> Self {
        Self { abi }
    }

    pub fn find_event_idexed_params(&self, _event: &str) -> Option<Vec<String>> {
        None
    }

    pub fn find_event_not_indexed_params(&self, event: &str) -> Result<Option<Vec<String>>> {
        if let Some(array) = self.abi.as_array() {
            let inputs = array
                .iter()
                .map(|item| serde_json::from_value::<AbiParams>(item.clone()))
                .filter(|item| item.is_ok())
                .map(|item| item.unwrap())
                .filter(|item| {
                    item.name.to_ascii_lowercase() == event.to_ascii_lowercase()
                        && item.param_type == "event"
                })
                .collect::<Vec<_>>();

            // TODO: can this result in more than one entry?
            if let Some(entry) = inputs.first() {
                Ok(Some(
                    entry
                        .inputs
                        .iter()
                        .filter(|item| !item.indexed)
                        .map(|item| item.name.clone())
                        .collect(),
                ))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
