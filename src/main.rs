// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::{Context, Result};
use quixote::{
    configuration::IndexerConfiguration,
    error_codes,
    indexing_app::IndexingApp,
    streamlit_wrapper::{FrontendOptions, start_frontend},
    telemetry,
};
use tracing::error;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let config = IndexerConfiguration::parse();

    // Setup the tracing subsystem.
    telemetry::setup_tracing(config.verbosity)?;

    // Run the indexing app.
    let app = IndexingApp::build_app(&config)
        .await
        .with_context(|| "Failed to build the indexing app")?;

    let indexing_task = tokio::spawn(async move {
        if let Err(e) = app.run().await {
            tracing::error!("Error running the indexing app: {:?}", e);
            std::process::exit(error_codes::ERROR_CODE_INDEXING_FAILED);
        }
    });

    // Launch the Streamlit frontend if not disabled.
    if !config.disable_frontend {
        tracing::info!("Launching frontend");
        let frontend_options = FrontendOptions {
            url: config.frontend_address.clone(),
            port: config.frontend_port,
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
