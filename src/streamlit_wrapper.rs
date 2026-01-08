// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Module that wraps the Python frontend developed using Streamlit.

use anyhow::{Result, anyhow};
use std::{
    env,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
    thread,
};
use tracing::{error, info, warn};

/// Options for the frontend.
pub struct FrontendOptions {
    pub url: String,
    pub port: u16,
    pub frontend_path: String,
    pub frontend_env_path: String,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            url: "127.0.0.1".to_string(),
            port: 8501,
            frontend_path: "frontend/generic_dashboard.py".to_string(),
            frontend_env_path: "quixote_frontend_env".to_string(),
        }
    }
}

/// Configures and starts the Python frontend based on Streamlit.
pub async fn start_frontend(options: FrontendOptions) -> Result<()> {
    // Use QUIXOTE_HOME if set, otherwise use current working directory
    let app_root: PathBuf = env::var("QUIXOTE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| env::current_dir().expect("Failed to get current directory"));

    let python_bin = app_root
        .join(&options.frontend_env_path)
        .join("bin")
        .join("python3");
    let script_path = app_root.join(&options.frontend_path);

    if !python_bin.exists() {
        return Err(anyhow!("Could not find bundled Python at {:?}", python_bin));
    }
    if !script_path.exists() {
        return Err(anyhow!(
            "Could not find frontend script at {:?}",
            script_path
        ));
    }

    // We use "python -m streamlit run frontend.py"
    // This is more robust than calling the 'streamlit' executable directly
    let mut child = Command::new(&python_bin)
        .arg("-m")
        .arg("streamlit")
        .arg("run")
        .arg(&script_path)
        // Optional: specific streamlit flags to make it look like a desktop app
        .arg("--server.headless=true")
        .arg("--global.developmentMode=false")
        // This avoids streamlit using colours in the log messages
        .arg("--theme.base=light")
        .arg(format!("--server.port={}", options.port))
        .arg(format!("--server.address={}", options.url))
        // Force Python to push log messages as they come rather than buffering
        .env("PYTHONUNBUFFERED", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start the Streamlit process");

    // Now, it is needed to capture the logs coming from Streamlit and tunnel these to tracing
    if let Some(stdout) = child.stdout.take() {
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        info!(target: "streamlit", "{}", l);
                    }
                    Err(e) => error!("Error reading Streamlit stdout: {}", e),
                }
            }
        });
    }

    if let Some(stderr) = child.stderr.take() {
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        warn!(target: "streamlit", "{}", l);
                    }
                    Err(e) => error!("Error reading Streamlit stderr: {}", e),
                }
            }
        });
    }

    // 6. Wait for the process to finish
    // This keeps the Rust binary alive as long as the Streamlit window is open.
    let _ = child.wait();

    info!("Streamlit app closed.");

    Ok(())
}
