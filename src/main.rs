// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::Result;
use etherduck::indexing_app::IndexingApp;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing()?;

    let app = IndexingApp::build_app()?;

    // Run the indexing app.
    app.run().await?;

    Ok(())
}

fn setup_tracing() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false).with_line_number(false))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .try_init()?;

    Ok(())
}
