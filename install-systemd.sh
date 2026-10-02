#!/usr/bin/env bash
# ==============================================================================
# Velda Edge - Systemd Installation Script (GitHub Releases)
#
# Downloads pre-compiled binary artifacts from GitHub Releases, creates the
# system service user/directories, and installs systemd service units.
#
# Run './install-systemd.sh --help' for complete usage information.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Output styling
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default values
GITHUB_REPO="phucle996/velda-edge"
RELEASE_TAG="latest"
GITHUB_TOKEN=""
ARCH_OVERRIDE=""
SKIP_CHECKSUM=false

COMPONENT="all" # all, edge, cp
INSTALL_PREFIX="/usr/local"
BIN_DIR=""
CONFIG_DIR="/etc/velda"
DATA_DIR="/var/lib/velda"
RUN_DIR="/run/velda"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="velda"
SERVICE_GROUP="velda"
CREATE_USER=true

BACKUP_OLD=false
OVERWRITE_CONFIG=false
FORCE=false

ENABLE_SERVICES=false
START_SERVICES=false
RESTART_SERVICES=false
CHECK_STATUS=false
NO_RELOAD=false

DO_UNINSTALL=false
DO_PURGE=false
DRY_RUN=false
VERBOSE=false
QUIET=false

# ------------------------------------------------------------------------------
# Logging & Helper Functions
# ------------------------------------------------------------------------------
log_info()    { [[ "$QUIET" == true ]] || echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { [[ "$QUIET" == true ]] || echo -e "${GREEN}[SUCCESS]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_debug()   { [[ "$VERBOSE" == true ]] && echo -e "${CYAN}[DEBUG]${NC} $*" || true; }

run_cmd() {
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${CYAN}[DRY-RUN]${NC} $*"
    else
        log_debug "Running: $*"
        "$@"
    fi
}

show_help() {
    echo -e "
${BOLD}================================================================================${NC}
${BOLD} Velda Edge - GitHub Releases Systemd Installer${NC}
${BOLD}================================================================================${NC}

${BOLD}USAGE:${NC}
    sudo ./install-systemd.sh [OPTIONS]

${BOLD}SOURCE & VERSION:${NC}
    -t, --tag <TAG>             GitHub release tag to install (e.g. v0.1.0). [Default: latest]
    -r, --repo <OWNER/REPO>     GitHub repository. [Default: phucle996/velda-edge]
    --token <TOKEN>             GitHub Personal Access Token for authenticated API requests
    --arch <amd64|arm64>        Override CPU architecture (Default: auto-detect)
    --no-checksum               Skip SHA256 checksum verification

${BOLD}TARGET COMPONENTS:${NC}
    --all                       Install all components (Edge Data Plane + Control Plane) [Default]
    --edge-only                 Install only velda-edge and velda-sync (Edge proxy stack)
    --cp-only,
    --control-plane-only        Install only velda-control-plane (Management API & UI)

${BOLD}DIRECTORIES & PERMISSIONS:${NC}
    --prefix <DIR>              Installation prefix [Default: /usr/local]
    --bin-dir <DIR>             Directory for binaries [Default: <prefix>/bin]
    --config-dir <DIR>          Directory for environment configs [Default: /etc/velda]
    --data-dir <DIR>            Directory for LKG storage [Default: /var/lib/velda]
    --run-dir <DIR>             Directory for IPC runtime sockets [Default: /run/velda]
    --systemd-dir <DIR>         Directory for systemd units [Default: /etc/systemd/system]
    --user <USER>               System user for service execution [Default: velda]
    --group <GROUP>             System group for service execution [Default: velda]
    --no-create-user            Do not create system user/group if they do not exist

${BOLD}BACKUP & OVERWRITE:${NC}
    --backup                    Backup existing binaries before replacing (.bak.<timestamp>)
    --overwrite-config          Overwrite existing /etc/velda/*.env with sample templates
    -f, --force                 Force install even if binaries already exist

${BOLD}SYSTEMD LIFECYCLE:${NC}
    --enable                    Enable services on boot (systemctl enable)
    --start                     Start services immediately after install
    --restart                   Restart services if already running
    --status                    Display service status after installation
    --no-reload                 Skip systemctl daemon-reload step

${BOLD}MAINTENANCE & DIAGNOSTICS:${NC}
    --uninstall                 Stop and remove service units and binaries
    --purge                     Purge EVERYTHING (binaries, units, configs, data, and user)
    --dry-run                   Simulate actions without writing files or calling systemctl
    -v, --verbose               Enable verbose command debugging
    -q, --quiet                 Quiet mode (suppress non-error output)
    -h, --help                  Show this help menu

${BOLD}EXAMPLES:${NC}
    sudo ./install-systemd.sh
    sudo ./install-systemd.sh -t v0.1.0 --edge-only --enable --start
    sudo ./install-systemd.sh --backup --restart
    sudo ./install-systemd.sh --uninstall
"
}

# ------------------------------------------------------------------------------
# Argument Parsing
# ------------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--tag)
            RELEASE_TAG="$2"
            shift 2
            ;;
        -r|--repo)
            GITHUB_REPO="$2"
            shift 2
            ;;
        --token)
            GITHUB_TOKEN="$2"
            shift 2
            ;;
        --arch)
            ARCH_OVERRIDE="$2"
            shift 2
            ;;
        --no-checksum)
            SKIP_CHECKSUM=true
            shift
            ;;
        --all)
            COMPONENT="all"
            shift
            ;;
        --edge-only)
            COMPONENT="edge"
            shift
            ;;
        --cp-only|--control-plane-only)
            COMPONENT="cp"
            shift
            ;;
        --prefix)
            INSTALL_PREFIX="$2"
            shift 2
            ;;
        --bin-dir)
            BIN_DIR="$2"
            shift 2
            ;;
        --config-dir)
            CONFIG_DIR="$2"
            shift 2
            ;;
        --data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR="$2"
            shift 2
            ;;
        --systemd-dir)
            SYSTEMD_DIR="$2"
            shift 2
            ;;
        --user)
            SERVICE_USER="$2"
            shift 2
            ;;
        --group)
            SERVICE_GROUP="$2"
            shift 2
            ;;
        --no-create-user)
            CREATE_USER=false
            shift
            ;;
        --backup)
            BACKUP_OLD=true
            shift
            ;;
        --overwrite-config)
            OVERWRITE_CONFIG=true
            shift
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        --enable)
            ENABLE_SERVICES=true
            shift
            ;;
        --start)
            START_SERVICES=true
            shift
            ;;
        --restart)
            RESTART_SERVICES=true
            shift
            ;;
        --status)
            CHECK_STATUS=true
            shift
            ;;
        --no-reload)
            NO_RELOAD=true
            shift
            ;;
        --uninstall)
            DO_UNINSTALL=true
            shift
            ;;
        --purge)
            DO_PURGE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -q|--quiet)
            QUIET=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

[[ -z "$BIN_DIR" ]] && BIN_DIR="${INSTALL_PREFIX}/bin"

# ------------------------------------------------------------------------------
# Uninstall / Purge Handler
# ------------------------------------------------------------------------------
if [[ "$DO_PURGE" == true || "$DO_UNINSTALL" == true ]]; then
    if [[ $EUID -ne 0 && "$DRY_RUN" == false ]]; then
        log_error "Uninstall/Purge requires root privileges. Please run with sudo."
        exit 1
    fi

    log_warn "Starting Velda Edge removal..."
    
    # 1. Stop and disable services
    UNITS=("velda-edge.service" "velda-sync.service" "velda-control-plane.service" "velda.target")
    for u in "${UNITS[@]}"; do
        if systemctl is-active --quiet "$u" 2>/dev/null; then
            log_info "Stopping $u..."
            run_cmd systemctl stop "$u" || true
        fi
        if systemctl is-enabled --quiet "$u" 2>/dev/null; then
            log_info "Disabling $u..."
            run_cmd systemctl disable "$u" || true
        fi
        if [[ -f "${SYSTEMD_DIR}/$u" ]]; then
            log_info "Removing unit: ${SYSTEMD_DIR}/$u"
            run_cmd rm -f "${SYSTEMD_DIR}/$u"
        fi
    done

    run_cmd systemctl daemon-reload || true

    # 2. Remove binaries
    for b in "velda-edge" "velda-sync" "velda-control-plane"; do
        if [[ -f "${BIN_DIR}/$b" ]]; then
            log_info "Removing binary: ${BIN_DIR}/$b"
            run_cmd rm -f "${BIN_DIR}/$b"
        fi
    done

    # 3. Purge configurations & data if requested
    if [[ "$DO_PURGE" == true ]]; then
        log_warn "Purging configuration (${CONFIG_DIR}) and data (${DATA_DIR})..."
        run_cmd rm -rf "$CONFIG_DIR" "$DATA_DIR" "$RUN_DIR"
        if getent passwd "$SERVICE_USER" >/dev/null 2>&1; then
            log_info "Removing system user: $SERVICE_USER"
            run_cmd userdel "$SERVICE_USER" || true
        fi
        if getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
            log_info "Removing system group: $SERVICE_GROUP"
            run_cmd groupdel "$SERVICE_GROUP" || true
        fi
        log_success "Velda Edge has been completely purged."
    else
        log_success "Velda Edge uninstalled (configs in ${CONFIG_DIR} and data in ${DATA_DIR} preserved)."
    fi
    exit 0
fi

# ------------------------------------------------------------------------------
# Pre-flight Checks
# ------------------------------------------------------------------------------
if [[ $EUID -ne 0 && "$DRY_RUN" == false ]]; then
    log_error "This script must be run as root (or with sudo)."
    exit 1
fi

command -v curl >/dev/null 2>&1 || { log_error "curl is required. Aborting."; exit 1; }
command -v tar >/dev/null 2>&1 || { log_error "tar is required. Aborting."; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { log_error "sha256sum is required. Aborting."; exit 1; }
command -v systemctl >/dev/null 2>&1 || { log_error "systemctl is required. Aborting."; exit 1; }

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$OS" != "linux" ]]; then
    log_error "Unsupported OS: $OS. Velda Edge requires Linux."
    exit 1
fi

if [[ -n "$ARCH_OVERRIDE" ]]; then
    ARCH_NAME="$ARCH_OVERRIDE"
else
    ARCH_RAW="$(uname -m)"
    case "$ARCH_RAW" in
        x86_64)         ARCH_NAME="amd64" ;;
        aarch64|arm64)  ARCH_NAME="arm64" ;;
        *)
            log_error "Unsupported architecture: $ARCH_RAW. Supported: amd64, arm64."
            exit 1
            ;;
    esac
fi

log_info "Target System:   ${BOLD}Linux/${ARCH_NAME}${NC}"
log_info "GitHub Repo:     ${BOLD}${GITHUB_REPO}${NC}"
log_info "Requested Tag:   ${BOLD}${RELEASE_TAG}${NC}"
log_info "Target Stack:    ${BOLD}${COMPONENT}${NC}"

# ------------------------------------------------------------------------------
# Release Asset Resolution
# ------------------------------------------------------------------------------
CURL_AUTH=()
if [[ -n "$GITHUB_TOKEN" ]]; then
    CURL_AUTH=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

TMP_DIR="$(mktemp -d -t velda-install-XXXXXX)"
cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ "$RELEASE_TAG" == "latest" ]]; then
    log_info "Querying latest release metadata from GitHub API..."
    API_URL="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
    TAG_RESOLVED=$(curl -sSfL "${CURL_AUTH[@]}" "$API_URL" 2>/dev/null | grep -Po '"tag_name":\s*"\K[^"]+' || true)
    if [[ -z "$TAG_RESOLVED" ]]; then
        log_warn "Could not query GitHub API (rate-limit or private repo). Using direct 'latest/download' assets."
        DOWNLOAD_BASE="https://github.com/${GITHUB_REPO}/releases/latest/download"
    else
        RELEASE_TAG="$TAG_RESOLVED"
        log_info "Resolved latest release tag: ${BOLD}${RELEASE_TAG}${NC}"
        DOWNLOAD_BASE="https://github.com/${GITHUB_REPO}/releases/download/${RELEASE_TAG}"
    fi
else
    DOWNLOAD_BASE="https://github.com/${GITHUB_REPO}/releases/download/${RELEASE_TAG}"
fi

# ------------------------------------------------------------------------------
# Download & Verification
# ------------------------------------------------------------------------------
EDGE_ARCHIVE="velda-edge-linux-${ARCH_NAME}.tar.gz"
CP_ARCHIVE="velda-control-plane-linux-${ARCH_NAME}.tar.gz"
CHECKSUM_FILE="SHA256SUMS"

cd "$TMP_DIR"

if [[ "$SKIP_CHECKSUM" == false ]]; then
    log_info "Fetching ${CHECKSUM_FILE}..."
    if curl -sSfL "${CURL_AUTH[@]}" "${DOWNLOAD_BASE}/${CHECKSUM_FILE}" -o "${CHECKSUM_FILE}" 2>/dev/null; then
        log_debug "Found SHA256SUMS file."
    else
        log_warn "SHA256SUMS not published for release ${RELEASE_TAG}. Skipping checksum check."
        touch "${CHECKSUM_FILE}"
    fi
fi

download_and_extract() {
    local archive="$1"
    log_info "Downloading ${archive}..."
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${CYAN}[DRY-RUN]${NC} curl -fSL \"${DOWNLOAD_BASE}/${archive}\" -o \"${archive}\""
        echo -e "${CYAN}[DRY-RUN]${NC} tar -xzf \"${archive}\""
        return 0
    fi

    if ! curl -fSL "${CURL_AUTH[@]}" --progress-bar "${DOWNLOAD_BASE}/${archive}" -o "${archive}"; then
        log_error "Failed to download asset: ${DOWNLOAD_BASE}/${archive}"
        exit 1
    fi

    if [[ "$SKIP_CHECKSUM" == false ]] && grep -q "${archive}" "${CHECKSUM_FILE}" 2>/dev/null; then
        log_info "Verifying SHA256 for ${archive}..."
        grep "${archive}" "${CHECKSUM_FILE}" | sha256sum -c --status || {
            log_error "Checksum mismatch on ${archive}!"
            exit 1
        }
        log_success "Checksum verified for ${archive}"
    fi

    log_info "Extracting ${archive}..."
    tar -xzf "${archive}"
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    download_and_extract "$EDGE_ARCHIVE"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    download_and_extract "$CP_ARCHIVE"
fi

# ------------------------------------------------------------------------------
# System User & Directories
# ------------------------------------------------------------------------------
if [[ "$CREATE_USER" == true ]]; then
    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        log_info "Creating group: ${SERVICE_GROUP}"
        run_cmd groupadd -r "$SERVICE_GROUP"
    fi

    if ! getent passwd "$SERVICE_USER" >/dev/null 2>&1; then
        log_info "Creating user: ${SERVICE_USER}"
        run_cmd useradd -r -g "$SERVICE_GROUP" -d "$DATA_DIR" -s /usr/sbin/nologin -c "Velda Edge Service User" "$SERVICE_USER"
    fi
fi

log_info "Ensuring directories exist..."
run_cmd mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$CONFIG_DIR/config" "$DATA_DIR" "$DATA_DIR/runtime" "$DATA_DIR/staging" "$RUN_DIR"
run_cmd chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$DATA_DIR" "$RUN_DIR"
run_cmd chmod 750 "$DATA_DIR" "$DATA_DIR/runtime" "$DATA_DIR/staging"
run_cmd chmod 755 "$RUN_DIR"
run_cmd chmod 750 "$CONFIG_DIR"

# ------------------------------------------------------------------------------
# Install Binaries
# ------------------------------------------------------------------------------
install_bin() {
    local name="$1"
    local src="$TMP_DIR/$name"
    local dst="$BIN_DIR/$name"

    if [[ -f "$src" ]]; then
        if [[ -f "$dst" && "$BACKUP_OLD" == true ]]; then
            local bak="${dst}.bak.$(date +%Y%m%d%H%M%S)"
            log_info "Backing up old binary: ${dst} -> ${bak}"
            run_cmd cp -a "$dst" "$bak"
        fi

        log_info "Installing binary: ${dst}"
        run_cmd install -m 755 "$src" "$dst"
    elif [[ "$FORCE" == false ]]; then
        log_debug "Binary $name not found in unpacked files; skipping."
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    install_bin "velda-edge"
    install_bin "velda-sync"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    install_bin "velda-control-plane"
fi

# ------------------------------------------------------------------------------
# Install Environment Templates
# ------------------------------------------------------------------------------
install_env() {
    local env_name="$1"
    local sample="${SCRIPT_DIR}/systemd/env/${env_name}.example"
    local target="${CONFIG_DIR}/${env_name}"

    if [[ -f "$target" && "$OVERWRITE_CONFIG" == false ]]; then
        log_warn "Config ${target} already exists. Preserving (use --overwrite-config to replace)."
        return 0
    fi

    if [[ -f "$sample" ]]; then
        log_info "Installing config: ${target}"
        run_cmd cp "$sample" "$target"
    else
        log_info "Downloading config template: ${env_name}.example..."
        local raw_url="https://raw.githubusercontent.com/${GITHUB_REPO}/${RELEASE_TAG}/systemd/env/${env_name}.example"
        if [[ "$DRY_RUN" == true ]]; then
            echo -e "${CYAN}[DRY-RUN]${NC} curl -sSfL \"$raw_url\" -o \"$target\""
        else
            curl -sSfL "${CURL_AUTH[@]}" "$raw_url" -o "$target" 2>/dev/null || \
            curl -sSfL "${CURL_AUTH[@]}" "https://raw.githubusercontent.com/${GITHUB_REPO}/main/systemd/env/${env_name}.example" -o "$target" || true
        fi
    fi

    if [[ -f "$target" ]]; then
        run_cmd chown "root:${SERVICE_GROUP}" "$target"
        run_cmd chmod 640 "$target"
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    install_env "edge.env"
    install_env "sync.env"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    install_env "control-plane.env"
fi

# ------------------------------------------------------------------------------
# Install Systemd Units
# ------------------------------------------------------------------------------
install_systemd_unit() {
    local unit_name="$1"
    local src="${SCRIPT_DIR}/systemd/${unit_name}"
    local dst="${SYSTEMD_DIR}/${unit_name}"

    if [[ -f "$src" ]]; then
        log_info "Installing unit: ${dst}"
        run_cmd cp "$src" "$dst"
        run_cmd chmod 644 "$dst"
    else
        log_info "Downloading systemd unit: ${unit_name}..."
        local raw_url="https://raw.githubusercontent.com/${GITHUB_REPO}/${RELEASE_TAG}/systemd/${unit_name}"
        if [[ "$DRY_RUN" == true ]]; then
            echo -e "${CYAN}[DRY-RUN]${NC} curl -sSfL \"$raw_url\" -o \"$dst\""
        else
            if curl -sSfL "${CURL_AUTH[@]}" "$raw_url" -o "$dst" 2>/dev/null || \
               curl -sSfL "${CURL_AUTH[@]}" "https://raw.githubusercontent.com/${GITHUB_REPO}/main/systemd/${unit_name}" -o "$dst"; then
                run_cmd chmod 644 "$dst"
            else
                log_warn "Failed to retrieve unit file: ${unit_name}"
            fi
        fi
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    install_systemd_unit "velda-edge.service"
    install_systemd_unit "velda-sync.service"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    install_systemd_unit "velda-control-plane.service"
fi

if [[ -f "${SCRIPT_DIR}/systemd/velda.target" ]] || [[ "$COMPONENT" == "all" ]]; then
    install_systemd_unit "velda.target"
fi

if [[ "$NO_RELOAD" == false ]]; then
    log_info "Reloading systemd daemon..."
    run_cmd systemctl daemon-reload
fi

# ------------------------------------------------------------------------------
# Service Lifecycle Control
# ------------------------------------------------------------------------------
ACTIVE_UNITS=()
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    ACTIVE_UNITS+=("velda-edge.service" "velda-sync.service")
fi
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    ACTIVE_UNITS+=("velda-control-plane.service")
fi

if [[ "$ENABLE_SERVICES" == true ]]; then
    log_info "Enabling services: ${ACTIVE_UNITS[*]}"
    run_cmd systemctl enable "${ACTIVE_UNITS[@]}"
fi

if [[ "$RESTART_SERVICES" == true ]]; then
    log_info "Restarting services: ${ACTIVE_UNITS[*]}"
    run_cmd systemctl restart "${ACTIVE_UNITS[@]}"
elif [[ "$START_SERVICES" == true ]]; then
    log_info "Starting services: ${ACTIVE_UNITS[*]}"
    run_cmd systemctl start "${ACTIVE_UNITS[@]}"
fi

if [[ "$CHECK_STATUS" == true && "$DRY_RUN" == false ]]; then
    echo ""
    log_info "${BOLD}Active Service Status:${NC}"
    for u in "${ACTIVE_UNITS[@]}"; do
        systemctl status "$u" --no-pager || true
        echo ""
    done
fi

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------
echo ""
echo -e "${GREEN}${BOLD}================================================================${NC}"
echo -e "${GREEN}${BOLD} Velda Edge Installation Successful!${NC}"
echo -e "${GREEN}${BOLD}================================================================${NC}"
echo -e "Version Tag:       ${BOLD}${RELEASE_TAG}${NC}"
echo -e "Binaries:          ${BOLD}${BIN_DIR}${NC}"
echo -e "Configs:           ${BOLD}${CONFIG_DIR}/*.env${NC}"
echo -e "Data Directory:    ${BOLD}${DATA_DIR}${NC}"
echo ""
echo -e "Next steps:"
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    echo -e "  Start Edge:      ${BOLD}sudo systemctl start velda-edge velda-sync${NC}"
    echo -e "  View Edge Logs:  ${BOLD}sudo journalctl -u velda-edge -f${NC}"
fi
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    echo -e "  Start CP:        ${BOLD}sudo systemctl start velda-control-plane${NC}"
    echo -e "  View CP Logs:    ${BOLD}sudo journalctl -u velda-control-plane -f${NC}"
fi
if [[ -f "${SYSTEMD_DIR}/velda.target" ]]; then
    echo -e "  Unified Target:  ${BOLD}sudo systemctl start|stop|restart velda.target${NC}"
fi
echo ""
