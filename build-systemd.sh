#!/usr/bin/env bash
# ==============================================================================
# Velda Edge - Local Build & Systemd Installation Script
#
# Compiles binaries from the local source code repository (Rust crates and Go
# control-plane), and installs them as production systemd services.
#
# Run './build-systemd.sh --help' for complete usage information.
# ==============================================================================

set -euo pipefail

# Output styling
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Default values
COMPONENT="all"         # all, edge, cp, ui
BUILD_PROFILE="release" # release, debug
DO_STRIP=true
PARALLEL_JOBS=""
DO_CLEAN=false

SKIP_UI=false
NO_INSTALL=false
BACKUP_OLD=false
OVERWRITE_CONFIG=false

INSTALL_PREFIX="/usr/local"
BIN_DIR=""
CONFIG_DIR="/etc/velda"
DATA_DIR="/var/lib/velda"
RUN_DIR="/run/velda"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="velda"
SERVICE_GROUP="velda"
CREATE_USER=true

ENABLE_SERVICES=false
START_SERVICES=false
RESTART_SERVICES=false
CHECK_STATUS=false
NO_RELOAD=false

CHECK_ONLY=false
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

# Sudo wrapper for privileged system actions
run_elevated() {
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${CYAN}[DRY-RUN sudo]${NC} $*"
    else
        log_debug "Elevated run: $*"
        if [[ $EUID -eq 0 ]]; then
            "$@"
        else
            sudo "$@"
        fi
    fi
}

run_user() {
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
${BOLD} Velda Edge - Local Source Build & Systemd Installer${NC}
${BOLD}================================================================================${NC}

${BOLD}USAGE:${NC}
    ./build-systemd.sh [OPTIONS]

${BOLD}BUILD TARGETS:${NC}
    --all                       Build all components (Edge, Sync, Control Plane + UI) [Default]
    --edge-only                 Build only velda-edge and velda-sync (Rust stack)
    --cp-only,
    --control-plane-only        Build only velda-control-plane (Go + Web Console)
    --skip-ui                   Skip npm UI build step (use existing ui/dist bundle)
    --ui-only                   Build only the React Web Console bundle

${BOLD}BUILD PROFILES & COMPILER OPTIONS:${NC}
    --release                   Build with release optimizations [Default]
    --debug, --dev              Build in debug mode for rapid testing & debugging
    --no-strip                  Do not strip debug symbols from output binaries
    -j, --jobs <N>              Set parallel build jobs count (passed to cargo & go)
    --clean                     Clean cargo and go caches before compiling

${BOLD}INSTALLATION & SYSTEMD:${NC}
    --no-install                Compile binaries only; do not install to system or touch systemd
    --backup                    Backup existing binaries before replacing (.bak.<timestamp>)
    --overwrite-config          Overwrite existing /etc/velda/*.env with sample templates
    --prefix <DIR>              Installation prefix [Default: /usr/local]
    --bin-dir <DIR>             Directory for binaries [Default: <prefix>/bin]
    --config-dir <DIR>          Directory for environment configs [Default: /etc/velda]
    --data-dir <DIR>            Directory for LKG storage [Default: /var/lib/velda]
    --run-dir <DIR>             Directory for IPC runtime sockets [Default: /run/velda]
    --systemd-dir <DIR>         Directory for systemd units [Default: /etc/systemd/system]
    --user <USER>               System user for service execution [Default: velda]
    --group <GROUP>             System group for service execution [Default: velda]
    --no-create-user            Do not create system user/group
    --enable                    Enable services on system boot (systemctl enable)
    --start                     Start services immediately after install
    --restart                   Restart services after installation (great for code reload)
    --status                    Display service status after deployment
    --no-reload                 Skip systemctl daemon-reload step

${BOLD}DIAGNOSTICS & MISC:${NC}
    --check-only                Verify build toolchains (cargo, go, node, protoc) and exit
    --dry-run                   Simulate build and install steps without modifying system
    -v, --verbose               Enable verbose command debugging
    -q, --quiet                 Suppress non-error logging
    -h, --help                  Show this help menu

${BOLD}EXAMPLES:${NC}
    ./build-systemd.sh
    ./build-systemd.sh --edge-only --restart
    ./build-systemd.sh --no-install --debug
    ./build-systemd.sh --all --clean --jobs 4 --enable --start
"
}

# ------------------------------------------------------------------------------
# Argument Parsing
# ------------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
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
        --ui-only)
            COMPONENT="ui"
            shift
            ;;
        --skip-ui)
            SKIP_UI=true
            shift
            ;;
        --release)
            BUILD_PROFILE="release"
            shift
            ;;
        --debug|--dev)
            BUILD_PROFILE="debug"
            shift
            ;;
        --no-strip)
            DO_STRIP=false
            shift
            ;;
        -j|--jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --clean)
            DO_CLEAN=true
            shift
            ;;
        --no-install)
            NO_INSTALL=true
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
        --check-only)
            CHECK_ONLY=true
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ------------------------------------------------------------------------------
# Toolchain Verification
# ------------------------------------------------------------------------------
log_info "Verifying required development toolchains..."

TOOL_ERRORS=0
check_tool() {
    local cmd="$1"
    local desc="$2"
    if command -v "$cmd" >/dev/null 2>&1; then
        log_debug "Found $cmd: $(command -v "$cmd")"
    else
        log_error "Missing required tool: ${BOLD}$cmd${NC} ($desc)"
        TOOL_ERRORS=$((TOOL_ERRORS + 1))
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    check_tool "cargo" "Rust package manager"
    check_tool "rustc" "Rust compiler"
    check_tool "protoc" "Protobuf compiler (apt install protobuf-compiler)"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    check_tool "go" "Go compiler"
    if [[ "$SKIP_UI" == false && "$COMPONENT" != "edge" ]]; then
        check_tool "node" "Node.js runtime"
        check_tool "npm" "Node package manager"
    fi
fi

if [[ "$COMPONENT" == "ui" ]]; then
    check_tool "node" "Node.js runtime"
    check_tool "npm" "Node package manager"
fi

if [[ $TOOL_ERRORS -gt 0 ]]; then
    log_error "Prerequisite check failed ($TOOL_ERRORS missing tools). Aborting."
    exit 1
fi

log_success "All required build toolchains are available."

if [[ "$CHECK_ONLY" == true ]]; then
    log_success "Toolchain check completed (--check-only requested). Exiting."
    exit 0
fi

# ------------------------------------------------------------------------------
# Build Clean (Optional)
# ------------------------------------------------------------------------------
if [[ "$DO_CLEAN" == true ]]; then
    log_info "Cleaning previous build artifacts..."
    run_user cargo clean || true
    (cd control-plane && run_user go clean || true)
    log_success "Clean completed."
fi

# Determine output directory
if [[ "$BUILD_PROFILE" == "release" ]]; then
    TARGET_OUT="target/release"
    CARGO_FLAGS=("--release")
else
    TARGET_OUT="target/debug"
    CARGO_FLAGS=()
fi

if [[ -n "$PARALLEL_JOBS" ]]; then
    CARGO_FLAGS+=("-j" "$PARALLEL_JOBS")
fi

mkdir -p "$TARGET_OUT"

# ------------------------------------------------------------------------------
# 1. Build UI Console (if applicable)
# ------------------------------------------------------------------------------
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" || "$COMPONENT" == "ui" ]]; then
    if [[ "$SKIP_UI" == false ]]; then
        log_info "${BOLD}Building UI Console bundle (Vite/React)...${NC}"
        (
            cd ui
            run_user npm ci --prefer-offline 2>/dev/null || run_user npm install
            run_user npm run build
        )
        run_user mkdir -p control-plane/internal/console/dist
        run_user rm -rf control-plane/internal/console/dist/*
        run_user cp -r ui/dist/* control-plane/internal/console/dist/ 2>/dev/null || true
        log_success "UI Console compiled into control-plane/internal/console/dist"
    else
        log_info "Skipping UI build (--skip-ui specified)."
    fi

    if [[ "$COMPONENT" == "ui" ]]; then
        log_success "UI build complete."
        exit 0
    fi
fi

# ------------------------------------------------------------------------------
# 2. Build Edge Data Plane & Sync (Rust)
# ------------------------------------------------------------------------------
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    log_info "${BOLD}Compiling Rust crates: velda-edge, velda-sync (${BUILD_PROFILE} profile)...${NC}"
    run_user cargo build "${CARGO_FLAGS[@]}" -p velda-edge -p velda-sync

    if [[ "$DO_STRIP" == true && "$BUILD_PROFILE" == "release" ]]; then
        if command -v strip >/dev/null 2>&1; then
            log_info "Stripping symbols from Rust release binaries..."
            run_user strip "${TARGET_OUT}/velda-edge" "${TARGET_OUT}/velda-sync" 2>/dev/null || true
        fi
    fi
    log_success "Built ${TARGET_OUT}/velda-edge and ${TARGET_OUT}/velda-sync"
fi

# ------------------------------------------------------------------------------
# 3. Build Control Plane (Go)
# ------------------------------------------------------------------------------
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    log_info "${BOLD}Compiling Go control-plane binary...${NC}"
    GO_BUILD_FLAGS=(-trimpath)
    if [[ "$BUILD_PROFILE" == "release" ]]; then
        GO_BUILD_FLAGS+=(-ldflags="-s -w")
    fi
    if [[ -n "$PARALLEL_JOBS" ]]; then
        GO_BUILD_FLAGS+=(-p "$PARALLEL_JOBS")
    fi

    (
        cd control-plane
        run_user env CGO_ENABLED=0 go build "${GO_BUILD_FLAGS[@]}" -o "../${TARGET_OUT}/velda-control-plane" ./cmd
    )
    log_success "Built ${TARGET_OUT}/velda-control-plane"
fi

# Exit early if only compilation was requested
if [[ "$NO_INSTALL" == true ]]; then
    log_success "Compilation finished (--no-install). Artifacts are located in ${BOLD}${TARGET_OUT}/${NC}"
    exit 0
fi

# ------------------------------------------------------------------------------
# 4. Systemd Installation
# ------------------------------------------------------------------------------
log_info "Installing binaries and system configuration..."

# 4.1 Create System User and Group
if [[ "$CREATE_USER" == true ]]; then
    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        log_info "Creating group: ${SERVICE_GROUP}"
        run_elevated groupadd -r "$SERVICE_GROUP"
    fi

    if ! getent passwd "$SERVICE_USER" >/dev/null 2>&1; then
        log_info "Creating user: ${SERVICE_USER}"
        run_elevated useradd -r -g "$SERVICE_GROUP" -d "$DATA_DIR" -s /usr/sbin/nologin -c "Velda Edge Service User" "$SERVICE_USER"
    fi
fi

# 4.2 Storage & Runtime Directories
run_elevated mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$CONFIG_DIR/config" "$DATA_DIR" "$DATA_DIR/runtime" "$DATA_DIR/staging" "$RUN_DIR"
run_elevated chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "$DATA_DIR" "$RUN_DIR"
run_elevated chmod 750 "$DATA_DIR" "$DATA_DIR/runtime" "$DATA_DIR/staging"
run_elevated chmod 755 "$RUN_DIR"
run_elevated chmod 750 "$CONFIG_DIR"

# 4.3 Install Binaries
install_binary() {
    local bin_name="$1"
    local src="${TARGET_OUT}/${bin_name}"
    local dst="${BIN_DIR}/${bin_name}"

    if [[ -f "$src" ]]; then
        if [[ -f "$dst" && "$BACKUP_OLD" == true ]]; then
            local bak="${dst}.bak.$(date +%Y%m%d%H%M%S)"
            log_info "Backing up old binary: ${dst} -> ${bak}"
            run_elevated cp -a "$dst" "$bak"
        fi

        log_info "Installing: ${dst}"
        run_elevated install -m 755 "$src" "$dst"
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    install_binary "velda-edge"
    install_binary "velda-sync"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    install_binary "velda-control-plane"
fi

# 4.4 Install Sample Environment Configs
install_env_config() {
    local env_name="$1"
    local sample="${SCRIPT_DIR}/systemd/env/${env_name}.example"
    local target="${CONFIG_DIR}/${env_name}"

    if [[ -f "$target" && "$OVERWRITE_CONFIG" == false ]]; then
        log_warn "Config ${target} already exists. Preserving (use --overwrite-config to replace)."
    elif [[ -f "$sample" ]]; then
        log_info "Installing config: ${target}"
        run_elevated cp "$sample" "$target"
        run_elevated chown "root:${SERVICE_GROUP}" "$target"
        run_elevated chmod 640 "$target"
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    install_env_config "edge.env"
    install_env_config "sync.env"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    install_env_config "control-plane.env"
fi

# 4.5 Install Systemd Units
install_systemd_unit() {
    local unit_name="$1"
    local src="${SCRIPT_DIR}/systemd/${unit_name}"
    local dst="${SYSTEMD_DIR}/${unit_name}"

    if [[ -f "$src" ]]; then
        log_info "Installing unit: ${dst}"
        run_elevated cp "$src" "$dst"
        run_elevated chmod 644 "$dst"
    fi
}

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    install_systemd_unit "velda-edge.service"
    install_systemd_unit "velda-sync.service"
fi

if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    install_systemd_unit "velda-control-plane.service"
fi

if [[ -f "${SCRIPT_DIR}/systemd/velda.target" ]]; then
    install_systemd_unit "velda.target"
fi

if [[ "$NO_RELOAD" == false ]]; then
    log_info "Reloading systemd daemon..."
    run_elevated systemctl daemon-reload
fi

# 4.6 Service Lifecycle Management
ACTIVE_UNITS=()
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "edge" ]]; then
    ACTIVE_UNITS+=("velda-edge.service" "velda-sync.service")
fi
if [[ "$COMPONENT" == "all" || "$COMPONENT" == "cp" ]]; then
    ACTIVE_UNITS+=("velda-control-plane.service")
fi

if [[ "$ENABLE_SERVICES" == true ]]; then
    log_info "Enabling services: ${ACTIVE_UNITS[*]}"
    run_elevated systemctl enable "${ACTIVE_UNITS[@]}"
fi

if [[ "$RESTART_SERVICES" == true ]]; then
    log_info "Restarting services: ${ACTIVE_UNITS[*]}"
    run_elevated systemctl restart "${ACTIVE_UNITS[@]}"
elif [[ "$START_SERVICES" == true ]]; then
    log_info "Starting services: ${ACTIVE_UNITS[*]}"
    run_elevated systemctl start "${ACTIVE_UNITS[@]}"
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
echo -e "${GREEN}${BOLD} Velda Edge Local Build & Deployment Successful!${NC}"
echo -e "${GREEN}${BOLD}================================================================${NC}"
echo -e "Compiled Output:   ${BOLD}${TARGET_OUT}${NC}"
echo -e "Installed Bin:     ${BOLD}${BIN_DIR}${NC}"
echo -e "Configs:           ${BOLD}${CONFIG_DIR}/*.env${NC}"
echo -e "LKG Data Storage:  ${BOLD}${DATA_DIR}${NC}"
echo ""
echo -e "Useful Commands:"
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
