// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::{Context, Result};
use clap::Parser;
use etherduck::{cli::IndexingArgs, indexing_app::IndexingApp};
use etherduck::{
    error_codes,
    streamlit_wrapper::{FrontendOptions, start_frontend},
};
use tracing::error;
use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let args = IndexingArgs::parse();
    // Setup the tracing subsystem.
    setup_tracing(args.verbosity)?;

    let disable_frontend = args.disable_frontend;

    // Run the indexing app.
    let app = IndexingApp::build_app(args).with_context(|| "Failed to build the indexing app")?;

    let indexing_task = tokio::spawn(async move {
        if let Err(e) = app.run().await {
            tracing::error!("Error running the indexing app: {:?}", e);
            std::process::exit(error_codes::ERROR_CODE_INDEXING_FAILED);
        }
    });

    // Launch the Streamlit frontend if not disabled.
    if !disable_frontend {
        tracing::info!("Launching frontend");
        // TODO: Add options to configure the frontend.
        if let Err(e) = start_frontend(FrontendOptions::default()).await {
            error!("{e}");
            error!(
                "The frontend is disabled due to an error. Please restart the application to launch again the frontend."
            );
        }
    }

    let _ = tokio::join!(indexing_task);

    Ok(())
}

fn setup_tracing(verbosity: u8) -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true).with_line_number(false))
        .with(match verbosity {
            0 => LevelFilter::WARN,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        })
        .try_init()?;

    Ok(())
}
