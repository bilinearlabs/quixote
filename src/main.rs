// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::Result;
use clap::Parser;
use etherduck::{cli::IndexingArgs, indexing_app::IndexingApp};
use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let args = IndexingArgs::parse();
    setup_tracing(args.verbosity)?;

    let app = IndexingApp::build_app(args)?;

    // Run the indexing app.
    app.run().await?;

    Ok(())
}

fn setup_tracing(verbosity: u8) -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false).with_line_number(false))
        .with(match verbosity {
            0 => LevelFilter::WARN,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        })
        .try_init()?;

    Ok(())
}
