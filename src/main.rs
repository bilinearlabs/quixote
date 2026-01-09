// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

use anyhow::Result;
use quixote::{
    configuration::IndexerConfiguration,
    error_codes,
    indexing_app::IndexingApp,
    streamlit_wrapper::{FrontendOptions, start_frontend},
    telemetry,
};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let config = IndexerConfiguration::parse();

    // Setup the tracing subsystem.
    telemetry::setup_tracing(config.verbosity)?;

    info!(
        "Starting Quixote indexer by Bilinear Labs - version {}",
        env!("CARGO_PKG_VERSION")
    );

    println!();
    println!(
        "  ╭──────────────────────────────────────────────────────────────────────────────────╮"
    );
    println!(
        "  │  Don Quijote de la Mancha · Segunda parte · Capítulo XLII                        │"
    );
    println!(
        "  │  \"De los consejos que dio don Quijote a Sancho Panza antes que fuese             │"
    );
    println!(
        "  │   a gobernar la ínsula\"                                                          │"
    );
    println!(
        "  ├──────────────────────────────────────────────────────────────────────────────────┤"
    );
    println!(
        "  │                                                                                  │"
    );
    println!(
        "  │  \"Haz gala, Sancho, de la humildad de tu linaje, y no te desprecies de decir     │"
    );
    println!(
        "  │   que vienes de labradores, porque viendo que no te corres, ninguno se pondrá    │"
    );
    println!(
        "  │   a correrte, y préciate más de ser humilde virtuoso que pecador soberbio.       │"
    );
    println!(
        "  │   Inumerables son aquellos que de baja estirpe nacidos, han subido a la suma     │"
    );
    println!(
        "  │   dignidad pontificia e imperatoria; y desta verdad te pudiera traer tantos      │"
    );
    println!(
        "  │   ejemplos, que te cansaran.\"                                                    │"
    );
    println!(
        "  │                                                                                  │"
    );
    println!(
        "  ╰──────────────────────────────────────────────────────────────────────────────────╯"
    );
    println!();

    // Run the indexing app.
    let app = IndexingApp::build_app(&config).await.unwrap_or_else(|e| {
        error!("Failed to build the indexing app: {e}");
        std::process::exit(error_codes::ERROR_CODE_WRONG_INPUT_ARGUMENTS);
    });

    let indexing_task = tokio::spawn(async move {
        if let Err(e) = app.run().await {
            error!("Error running the indexing app: {e}");
            std::process::exit(error_codes::ERROR_CODE_INDEXING_FAILED);
        }
    });

    // Launch the Streamlit frontend if not disabled.
    if !config.disable_frontend {
        info!("Launching frontend");
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
