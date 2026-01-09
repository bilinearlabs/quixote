#!/usr/bin/env bash
#
# Quixote Indexer - Installation Script
# https://github.com/bilinearlabs/quixote
#
# Usage:
#   curl -sSfL https://raw.githubusercontent.com/bilinearlabs/quixote/main/script/install.bash | bash
#
# With a specific version:
#   curl -sSfL https://raw.githubusercontent.com/bilinearlabs/quixote/main/script/install.bash | bash -s -- v0.1.0
#

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

REPO="bilinearlabs/quixote"
BINARY_NAME="quixote"
GITHUB_API="https://api.github.com"
GITHUB_RELEASES="https://github.com/${REPO}/releases"

# Data directory for frontend and assets
DATA_DIR_NAME=".quixote"

# Colors for output (disabled if not a terminal)
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    RESET='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' BOLD='' RESET=''
fi

# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────────────────────

info()    { echo -e "${BLUE}▸${RESET} $*"; }
success() { echo -e "${GREEN}✓${RESET} $*"; }
warn()    { echo -e "${YELLOW}⚠${RESET} $*"; }
error()   { echo -e "${RED}✗${RESET} $*" >&2; }
die()     { error "$@"; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────
# Platform Detection
# ─────────────────────────────────────────────────────────────────────────────

detect_platform() {
    local os arch

    # Detect OS
    case "$(uname -s)" in
        Linux*)  os="linux" ;;
        Darwin*) os="darwin" ;;
        *)       die "Unsupported operating system: $(uname -s)" ;;
    esac

    # Detect architecture
    case "$(uname -m)" in
        x86_64|amd64)  arch="x86_64" ;;
        arm64|aarch64) arch="aarch64" ;;
        *)             die "Unsupported architecture: $(uname -m)" ;;
    esac

    # Construct target triple
    case "${os}" in
        linux)  TARGET="${arch}-unknown-linux-gnu" ;;
        darwin) TARGET="${arch}-apple-darwin" ;;
    esac

    OS="${os}"
    ARCH="${arch}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Installation Path Selection
# ─────────────────────────────────────────────────────────────────────────────

determine_install_dir() {
    # If running as root, use system-wide path
    if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
        INSTALL_DIR="/usr/local/bin"
    else
        # For regular users, use ~/.local/bin (XDG compliant)
        INSTALL_DIR="${HOME}/.local/bin"
    fi

    # Create directory if it doesn't exist
    if [[ ! -d "${INSTALL_DIR}" ]]; then
        info "Creating installation directory: ${INSTALL_DIR}"
        mkdir -p "${INSTALL_DIR}"
    fi

    # Verify the directory is writable
    if [[ ! -w "${INSTALL_DIR}" ]]; then
        die "Cannot write to ${INSTALL_DIR}. Try running with sudo or choose a different location."
    fi

    # Check if install dir is in PATH
    if [[ ":${PATH}:" != *":${INSTALL_DIR}:"* ]]; then
        warn "${INSTALL_DIR} is not in your PATH"
        warn "Add it to your shell profile: export PATH=\"\$PATH:${INSTALL_DIR}\""
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Data Directory Selection (for frontend and assets)
# ─────────────────────────────────────────────────────────────────────────────

determine_data_dir() {
    # If running as root, use system-wide path
    if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
        DATA_DIR="/usr/local/share/${BINARY_NAME}"
    else
        # For regular users, use ~/.quixote (XDG compliant would be ~/.local/share/quixote)
        DATA_DIR="${HOME}/${DATA_DIR_NAME}"
    fi

    # Create directory if it doesn't exist
    if [[ ! -d "${DATA_DIR}" ]]; then
        info "Creating data directory: ${DATA_DIR}"
        mkdir -p "${DATA_DIR}"
    fi

    # Verify the directory is writable
    if [[ ! -w "${DATA_DIR}" ]]; then
        die "Cannot write to ${DATA_DIR}. Try running with sudo or choose a different location."
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Version Management
# ─────────────────────────────────────────────────────────────────────────────

get_latest_version() {
    local version

    version=$(curl -sSf "${GITHUB_API}/repos/${REPO}/releases/latest" 2>/dev/null \
        | grep '"tag_name":' \
        | sed -E 's/.*"([^"]+)".*/\1/')

    if [[ -z "${version}" ]]; then
        die "Failed to fetch latest version. Check your network connection or specify a version manually."
    fi

    echo "${version}"
}

get_installed_version() {
    local binary_path="${INSTALL_DIR}/${BINARY_NAME}"

    if [[ -x "${binary_path}" ]]; then
        # Try to get version from binary
        "${binary_path}" --version 2>/dev/null | head -1 | awk '{print $2}' || echo "unknown"
    else
        echo ""
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Download and Install
# ─────────────────────────────────────────────────────────────────────────────

download_and_install() {
    local version="$1"
    local archive_name="${BINARY_NAME}-${version}-${TARGET}.tar.gz"
    local download_url="${GITHUB_RELEASES}/download/${version}/${archive_name}"
    local checksum_url="${download_url}.sha256"
    local temp_dir

    # Create temporary directory
    temp_dir=$(mktemp -d)
    trap "rm -rf ${temp_dir}" EXIT

    info "Downloading ${BINARY_NAME} ${version} for ${TARGET}..."

    # Download archive
    if ! curl -sSfL "${download_url}" -o "${temp_dir}/${archive_name}"; then
        die "Failed to download ${archive_name}. The version or platform may not be available."
    fi

    # Download and verify checksum
    info "Verifying checksum..."
    if curl -sSfL "${checksum_url}" -o "${temp_dir}/${archive_name}.sha256" 2>/dev/null; then
        cd "${temp_dir}"
        if command -v sha256sum &>/dev/null; then
            sha256sum -c "${archive_name}.sha256" --quiet || die "Checksum verification failed!"
        elif command -v shasum &>/dev/null; then
            shasum -a 256 -c "${archive_name}.sha256" --quiet || die "Checksum verification failed!"
        else
            warn "No checksum tool available, skipping verification"
        fi
        cd - >/dev/null
    else
        warn "Checksum file not available, skipping verification"
    fi

    # Extract archive
    info "Extracting..."
    tar -xzf "${temp_dir}/${archive_name}" -C "${temp_dir}"

    # Find and install binary
    local binary_path
    binary_path=$(find "${temp_dir}" -name "${BINARY_NAME}" -type f | head -1)

    if [[ -z "${binary_path}" ]]; then
        die "Binary not found in archive"
    fi

    # Install binary
    chmod +x "${binary_path}"
    mv "${binary_path}" "${INSTALL_DIR}/${BINARY_NAME}"

    success "Installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"

    # Find and install frontend files
    local archive_dir
    archive_dir=$(find "${temp_dir}" -maxdepth 1 -type d -name "${BINARY_NAME}-*" | head -1)

    if [[ -n "${archive_dir}" ]]; then
        # Install frontend directory
        if [[ -d "${archive_dir}/frontend" ]]; then
            info "Installing frontend..."
            mkdir -p "${DATA_DIR}/frontend"
            cp -r "${archive_dir}/frontend/"* "${DATA_DIR}/frontend/"
            success "Installed frontend to ${DATA_DIR}/frontend/"
        fi

        # Install assets directory
        if [[ -d "${archive_dir}/assets" ]]; then
            info "Installing assets..."
            mkdir -p "${DATA_DIR}/assets"
            cp -r "${archive_dir}/assets/"* "${DATA_DIR}/assets/"
            success "Installed assets to ${DATA_DIR}/assets/"
        fi
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

main() {
    local requested_version="${1:-}"
    local installed_version
    local target_version

    echo ""
    echo -e "${BOLD}Quixote Indexer Installer${RESET}"
    echo "─────────────────────────────────"
    echo ""

    # Check for curl availability
    if ! command -v curl &>/dev/null; then
        die "curl is required but not found. Please install curl and try again."
    fi

    # Detect platform
    detect_platform
    info "Detected platform: ${OS} (${ARCH})"

    # Determine installation directory
    determine_install_dir
    info "Installation directory: ${INSTALL_DIR}"

    # Determine data directory for frontend and assets
    determine_data_dir
    info "Data directory: ${DATA_DIR}"

    # Determine target version
    if [[ -n "${requested_version}" ]]; then
        target_version="${requested_version}"
        # Ensure version starts with 'v'
        [[ "${target_version}" != v* ]] && target_version="v${target_version}"
    else
        info "Fetching latest version..."
        target_version=$(get_latest_version)
    fi
    info "Target version: ${target_version}"

    # Check for existing installation
    installed_version=$(get_installed_version)

    if [[ -n "${installed_version}" ]]; then
        if [[ "${installed_version}" == "${target_version#v}" ]] || [[ "v${installed_version}" == "${target_version}" ]]; then
            success "${BINARY_NAME} ${target_version} is already installed"
            echo ""
            exit 0
        else
            info "Found existing installation: ${installed_version}"
            info "Updating to ${target_version}..."
        fi
    fi

    # Download and install
    download_and_install "${target_version}"

    # Verify installation
    echo ""
    if "${INSTALL_DIR}/${BINARY_NAME}" --version &>/dev/null; then
        success "Installation complete!"
        echo ""
        echo -e "  ${BOLD}Configuration${RESET}"
        echo ""
        echo -e "  To enable the frontend dashboard, set the QUIXOTE_HOME environment variable:"
        echo ""
        echo -e "    ${YELLOW}export QUIXOTE_HOME=\"${DATA_DIR}\"${RESET}"
        echo ""
        echo -e "  Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.) to persist it."
        echo ""
        echo -e "  ${BOLD}Run '${BINARY_NAME} --help' to get started${RESET}"
        echo ""
    else
        warn "Binary installed but verification failed. You may need to add ${INSTALL_DIR} to your PATH."
    fi
}

main "$@"
