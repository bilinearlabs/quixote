// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::Result;
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt, prelude::*};

pub fn setup_tracing(verbosity: u8) -> Result<()> {
    let tracing_level = match verbosity {
        0 => Level::WARN,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false).with_line_number(false))
        .with(
            Targets::new()
                .with_target("quixote", tracing_level)
                .with_target("streamlit", tracing_level),
        )
        .try_init()?;

    Ok(())
}
