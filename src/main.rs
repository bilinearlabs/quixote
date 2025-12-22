// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::{Context, Result};
use clap::Parser;
use quixote::{
    cli::IndexingArgs,
    error_codes,
    indexing_app::IndexingApp,
    streamlit_wrapper::{FrontendOptions, start_frontend},
};
use tracing::{Level, error};
use tracing_subscriber::{filter::Targets, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let args = IndexingArgs::parse();
    // Setup the tracing subsystem.
    setup_tracing(args.verbosity)?;

    let disable_frontend = args.disable_frontend;
    let frontend_address = args.frontend_address.clone();
    let frontend_port = args.frontend_port;

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
        let frontend_options = FrontendOptions {
            url: frontend_address,
            port: frontend_port,
            ..Default::default()
        };

        if let Err(e) = start_frontend(frontend_options).await {
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
    let tracing_level = match verbosity {
        0 => Level::WARN,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true).with_line_number(false))
        .with(
            Targets::new()
                .with_target("quixote", tracing_level)
                .with_target("streamlit", tracing_level),
        )
        .try_init()?;

    Ok(())
}
