// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Library of the Etherduck crate.

pub mod event_collector_runner;
pub use event_collector_runner::*;
pub mod event_collector;
pub use event_collector::*;
pub mod storage_duckdb;
pub use storage_duckdb::*;

use alloy::{
    primitives::{Address, U256},
    transports::http::reqwest::Url,
};
use anyhow::Result;
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone)]
pub struct RpcHost {
    pub chain_id: u64,
    pub url: String,
    pub port: u16,
    pub username: Option<SecretString>,
    pub password: Option<SecretString>,
}

#[derive(Debug, Clone)]
pub enum Event {
    Erc20Event(Erc20Event),
    Erc721Event(Erc721Event),
}

#[derive(Debug, Clone)]
pub enum Erc20Event {
    Transfer(Address, Address, U256),
    Approval(Address, Address, U256),
    ApprovalForAll(Address, Address, bool),
}

#[derive(Debug, Clone)]
pub enum Erc721Event {
    Transfer(Address, Address, U256),
    Approval(Address, Address, U256),
    ApprovalForAll(Address, Address, bool),
}

impl std::str::FromStr for RpcHost {
    type Err = anyhow::Error;

    /// Parses the RPC host URL and returns a RpcHost struct.
    ///
    /// # Description
    ///
    /// The format of the RPC host URL is: <chain_id>[:<username>:<password>@]<host>:<port>.
    fn from_str(url: &str) -> Result<Self, Self::Err> {
        // Let's break down the input string in 2 parts: the initial data and the URL.
        let parts = url.split('@').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid RPC host URL: {}", url));
        }

        // Time to process the URL part.
        let raw_url = parts[1].split(':').collect::<Vec<&str>>();

        let port = if raw_url.len() != 3 {
            if raw_url[0].contains("https") {
                443
            } else {
                80
            }
        } else {
            raw_url[2].parse::<u16>()?
        };

        let url = format!("{}:{}", raw_url[0], raw_url[1]);

        // Time to process the initial data part.
        let init_part = parts[0].split(':').collect::<Vec<&str>>();
        let chain_id = init_part[0].parse::<u64>()?;

        let (username, password) = if init_part.len() > 1 {
            (
                Some(SecretString::from(init_part[1])),
                Some(SecretString::from(init_part[2])),
            )
        } else {
            (None, None)
        };

        Ok(RpcHost {
            chain_id,
            url,
            port,
            username,
            password,
        })
    }
}

impl TryInto<Url> for &RpcHost {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<Url> {
        let mut url = Url::parse(&format!("{}:{}", self.url, self.port))
            .map_err(|e| anyhow::anyhow!("Failed to create URL: {e}"))?;

        if let Some(username) = &self.username {
            url.set_username(username.expose_secret())
                .map_err(|e| anyhow::anyhow!("Failed to set username: {e:?}"))?;
        }
        if let Some(password) = &self.password {
            url.set_password(Some(password.expose_secret()))
                .map_err(|e| anyhow::anyhow!("Failed to set password: {e:?}"))?;
        }

        Ok(url)
    }
}
