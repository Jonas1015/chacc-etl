#!/bin/bash

# Deployment script for ChaCC ETL Pipeline Web UI
# This script handles deployment to Docker Hub and server updates

set -e

# Configuration
DOCKER_IMAGE="${DOCKER_IMAGE:-jonas1015/chacc-etl-pipeline}"
DOCKER_TAG="${DOCKER_TAG:-latest}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
ENV_FILE="${ENV_FILE:-.env}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required tools are installed
check_dependencies() {
    log_info "Checking dependencies..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed or not in PATH"
        exit 1
    fi

    log_success "Dependencies check passed"
}

# Build Docker image
build_image() {
    log_info "Building Docker image: $DOCKER_IMAGE:$DOCKER_TAG"

    if [ "$CI" = "true" ]; then
        log_info "Running in CI environment, using Docker build cache"
        docker build -t "$DOCKER_IMAGE:$DOCKER_TAG" .
    else
        docker build --no-cache -t "$DOCKER_IMAGE:$DOCKER_TAG" .
    fi

    log_success "Docker image built successfully"
}

# Push image to Docker Hub
push_image() {
    if [ -z "$DOCKERHUB_USERNAME" ] || [ -z "$DOCKERHUB_TOKEN" ]; then
        log_warning "Docker Hub credentials not provided, skipping push"
        return 0
    fi

    log_info "Pushing image to Docker Hub: $DOCKER_IMAGE:$DOCKER_TAG"

    echo "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin

    docker push "$DOCKER_IMAGE:$DOCKER_TAG"

    # Also push as latest if this is a release
    if [[ "$DOCKER_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        log_info "Tagging and pushing as latest"
        docker tag "$DOCKER_IMAGE:$DOCKER_TAG" "$DOCKER_IMAGE:latest"
        docker push "$DOCKER_IMAGE:latest"
    fi

    log_success "Image pushed to Docker Hub"
}

# Deploy using docker-compose
deploy_local() {
    log_info "Deploying locally with docker-compose"

    # Check if .env file exists
    if [ ! -f "$ENV_FILE" ]; then
        log_warning "Environment file $ENV_FILE not found, creating from example"
        if [ -f ".env.example" ]; then
            cp .env.example .env
            log_info "Created .env from .env.example - please update with your values"
        fi
    fi

    # Stop existing containers
    log_info "Stopping existing containers..."
    docker-compose -f "$COMPOSE_FILE" down || true

    # Start services
    log_info "Starting services..."
    docker-compose -f "$COMPOSE_FILE" up -d

    # Wait for services to be healthy
    log_info "Waiting for services to be ready..."
    sleep 30

    # Check if services are running
    if docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
        log_success "Deployment completed successfully"
        log_info "Application is running at http://localhost:5000"
        log_info "Luigi UI is available at http://localhost:8082"
    else
        log_error "Deployment failed - some services are not running"
        docker-compose -f "$COMPOSE_FILE" logs
        exit 1
    fi
}

# Health check
health_check() {
    log_info "Performing health check..."

    # Wait for web service to be ready
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -f -s http://localhost:5000 > /dev/null 2>&1; then
            log_success "Health check passed"
            return 0
        fi

        log_info "Waiting for service to be ready (attempt $attempt/$max_attempts)..."
        sleep 10
        ((attempt++))
    done

    log_error "Health check failed - service did not become ready"
    return 1
}

# Rollback function
rollback() {
    log_warning "Performing rollback..."

    # Stop current deployment
    docker-compose -f "$COMPOSE_FILE" down

    # Start previous version if available
    if docker images | grep -q "$DOCKER_IMAGE.*<none>"; then
        log_info "Starting previous version..."
        # This would need more complex logic for actual rollback
    fi

    log_error "Rollback completed - manual intervention may be required"
}

# Main deployment function
main() {
    local action="${1:-deploy}"

    case "$action" in
        "build")
            check_dependencies
            build_image
            ;;
        "push")
            check_dependencies
            push_image
            ;;
        "deploy")
            check_dependencies
            build_image
            deploy_local
            health_check
            ;;
        "rollback")
            rollback
            ;;
        *)
            echo "Usage: $0 {build|push|deploy|rollback}"
            echo "  build    - Build Docker image"
            echo "  push     - Push image to Docker Hub"
            echo "  deploy   - Full deployment (build + deploy locally)"
            echo "  rollback - Rollback to previous version"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"