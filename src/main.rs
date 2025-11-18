// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use anyhow::Result;
use etherduck::indexing_app::IndexingApp;

#[tokio::main]
async fn main() -> Result<()> {
    let app = IndexingApp::build_app()?;

    // Run the indexing app.
    app.run().await?;

    Ok(())
}
