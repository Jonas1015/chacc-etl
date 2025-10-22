#!/bin/bash

# Build script for ChaCC ETL Pipeline Web UI
# This script handles local Docker image building for testing
# CI/CD workflows handle automated building and pushing to Docker Hub

set -e

DOCKER_IMAGE="${DOCKER_IMAGE:-jonas1015/chacc-etl-pipeline}"
DOCKER_TAG="${DOCKER_TAG:-latest}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_dependencies() {
    log_info "Checking dependencies..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    log_success "Dependencies check passed"
}

build_image() {
    log_info "Building Docker image: $DOCKER_IMAGE:$DOCKER_TAG"

    docker build --no-cache -t "$DOCKER_IMAGE:$DOCKER_TAG" .

    log_success "Docker image built successfully"
    log_info "You can now test the image locally with: docker run -p 5000:5000 $DOCKER_IMAGE:$DOCKER_TAG"
}

main() {
    local action="${1:-build}"

    case "$action" in
        "build")
            check_dependencies
            build_image
            ;;
        *)
            echo "Usage: $0 {build}"
            echo "  build    - Build Docker image locally for testing"
            echo ""
            echo "Optional environment variables:"
            echo "  DOCKER_IMAGE - Docker image name (default: jonas1015/chacc-etl-pipeline)"
            echo "  DOCKER_TAG   - Docker image tag (default: latest)"
            echo ""
            echo "Note: Automated building and pushing to Docker Hub is handled by GitHub Actions workflows"
            exit 1
            ;;
    esac
}

main "$@"