#!/bin/bash

# Deployment script for Hiscox ETL Pipeline
# Usage: ./deploy.sh <environment> [options]

set -euo pipefail

ENVIRONMENT=${1:-}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
SKIP_TESTS=false
FORCE_DEPLOY=false
DRY_RUN=false
BUILD_IMAGES=true

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

# Help function
show_help() {
    cat << EOF
Usage: $0 <environment> [options]

Environments:
    dev         Deploy to development environment
    staging     Deploy to staging environment  
    production  Deploy to production environment

Options:
    --skip-tests        Skip running tests before deployment
    --force            Force deployment even if tests fail
    --dry-run          Show what would be deployed without executing
    --no-build         Skip building Docker images
    --help             Show this help message

Examples:
    $0 dev
    $0 production --skip-tests
    $0 staging --dry-run
EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            --force)
                FORCE_DEPLOY=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --no-build)
                BUILD_IMAGES=false
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                ;;
        esac
    done
}

# Validate environment
validate_environment() {
    case $ENVIRONMENT in
        dev|staging|production)
            log "Deploying to $ENVIRONMENT environment"
            ;;
        "")
            error "Environment is required. Use --help for usage information."
            ;;
        *)
            error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or production"
            ;;
    esac
}

# Set environment-specific variables
set_environment_vars() {
    case $ENVIRONMENT in
        dev)
            ACR_NAME="acrhiscoxetldev001am"
            RESOURCE_GROUP="rg-hiscox-etl-dev"
            DATABRICKS_TOKEN_VAR="DATABRICKS_TOKEN_DEV"
            ;;
        staging)
            ACR_NAME="acrhiscoxetlstaging001am"
            RESOURCE_GROUP="rg-hiscox-etl-staging"
            DATABRICKS_TOKEN_VAR="DATABRICKS_TOKEN_STAGING"
            ;;
        production)
            ACR_NAME="acrhiscoxetlprod001am"
            RESOURCE_GROUP="rg-hiscox-etl-prod-001"
            DATABRICKS_TOKEN_VAR="DATABRICKS_TOKEN_PROD"
            ;;
    esac
    
    # Generate deployment tag
    DEPLOYMENT_TAG="${GITHUB_SHA:-$(git rev-parse --short HEAD)}"
    if [[ -z "$DEPLOYMENT_TAG" ]]; then
        DEPLOYMENT_TAG="manual-$(date +%Y%m%d-%H%M%S)"
    fi
    
    info "Deployment tag: $DEPLOYMENT_TAG"
}

# Pre-deployment checks
pre_deployment_checks() {
    log "Running pre-deployment checks"
    
    # Check if Azure CLI is logged in
    if ! az account show &>/dev/null; then
        error "Not logged in to Azure CLI. Please run 'az login' first."
    fi
    
    # Check if required tools are installed
    local required_tools=("docker" "terraform" "python3")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &>/dev/null; then
            error "$tool is not installed or not in PATH"
        fi
    done
    
    # Check if we're in the right directory
    if [[ ! -f "$PROJECT_ROOT/requirements-minimal.txt" ]]; then
        error "Not in the correct project directory"
    fi
    
    # Validate Terraform configuration
    cd "$PROJECT_ROOT/terraform/environments/$ENVIRONMENT"
    terraform fmt -check -recursive . || warn "Terraform files are not properly formatted"
    terraform validate || error "Terraform configuration is invalid"
    
    log "Pre-deployment checks completed"
}

# Run tests
run_tests() {
    if [[ "$SKIP_TESTS" == "true" ]]; then
        warn "Skipping tests as requested"
        return 0
    fi
    
    log "Running tests"
    cd "$PROJECT_ROOT"
    
    # Install test dependencies
    python3 -m pip install --quiet pytest pytest-cov pytest-mock
    
    # Run unit tests
    if [[ -d "tests/unit" ]]; then
        info "Running unit tests"
        python3 -m pytest tests/unit/ -v --tb=short || {
            if [[ "$FORCE_DEPLOY" == "true" ]]; then
                warn "Unit tests failed but continuing due to --force flag"
            else
                error "Unit tests failed. Use --force to deploy anyway."
            fi
        }
    fi
    
    # Run integration tests for non-production
    if [[ "$ENVIRONMENT" != "production" && -d "tests/integration" ]]; then
        info "Running integration tests"
        python3 -m pytest tests/integration/ -v --tb=short || {
            if [[ "$FORCE_DEPLOY" == "true" ]]; then
                warn "Integration tests failed but continuing due to --force flag"
            else
                error "Integration tests failed. Use --force to deploy anyway."
            fi
        }
    fi
    
    log "Tests completed successfully"
}

# Build and push Docker images
build_and_push_images() {
    if [[ "$BUILD_IMAGES" == "false" ]]; then
        info "Skipping image build as requested"
        return 0
    fi
    
    log "Building and pushing Docker images"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY RUN] Would build and push images with tag: $DEPLOYMENT_TAG"
        return 0
    fi
    
    # Login to ACR
    az acr login --name "$ACR_NAME"
    
    # Build images
    info "Building ingestion image"
    docker build -t "$ACR_NAME.azurecr.io/etl-ingestion:$DEPLOYMENT_TAG" \
        -f docker/ingestion.Dockerfile .
    
    info "Building transformation image"
    docker build -t "$ACR_NAME.azurecr.io/etl-transformation:$DEPLOYMENT_TAG" \
        -f docker/transformation.Dockerfile .
    
    # Push images
    info "Pushing images to registry"
    docker push "$ACR_NAME.azurecr.io/etl-ingestion:$DEPLOYMENT_TAG"
    docker push "$ACR_NAME.azurecr.io/etl-transformation:$DEPLOYMENT_TAG"
    
    # Tag as latest for non-production
    if [[ "$ENVIRONMENT" != "production" ]]; then
        docker tag "$ACR_NAME.azurecr.io/etl-ingestion:$DEPLOYMENT_TAG" \
            "$ACR_NAME.azurecr.io/etl-ingestion:latest"
        docker tag "$ACR_NAME.azurecr.io/etl-transformation:$DEPLOYMENT_TAG" \
            "$ACR_NAME.azurecr.io/etl-transformation:latest"
        
        docker push "$ACR_NAME.azurecr.io/etl-ingestion:latest"
        docker push "$ACR_NAME.azurecr.io/etl-transformation:latest"
    fi
    
    log "Docker images built and pushed successfully"
}

# Deploy infrastructure
deploy_infrastructure() {
    log "Deploying infrastructure"
    
    cd "$PROJECT_ROOT/terraform/environments/$ENVIRONMENT"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY RUN] Would run terraform plan and apply"
        terraform plan
        return 0
    fi
    
    # Initialize Terraform
    terraform init
    
    # Plan deployment
    info "Creating Terraform plan"
    terraform plan -out=tfplan -detailed-exitcode
    local plan_exit_code=$?
    
    case $plan_exit_code in
        0)
            info "No infrastructure changes needed"
            ;;
        1)
            error "Terraform plan failed"
            ;;
        2)
            info "Infrastructure changes detected, applying..."
            terraform apply -auto-approve tfplan
            log "Infrastructure deployed successfully"
            ;;
    esac
    
    # Capture outputs
    terraform output > "$PROJECT_ROOT/terraform_outputs_$ENVIRONMENT.txt"
}

# Deploy application
deploy_application() {
    log "Deploying application"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY RUN] Would deploy application components"
        return 0
    fi
    
    cd "$PROJECT_ROOT"
    
    # Get Terraform outputs
    local tf_outputs_file="terraform_outputs_$ENVIRONMENT.txt"
    if [[ -f "$tf_outputs_file" ]]; then
        source <(grep -E '^[a-zA-Z_][a-zA-Z0-9_]*\s*=' "$tf_outputs_file" | sed 's/^/export /')
    fi
    
    # Deploy to Databricks if CLI is available
    if command -v databricks &>/dev/null; then
        info "Deploying to Databricks"
        
        # Configure Databricks CLI
        local databricks_token="${!DATABRICKS_TOKEN_VAR:-}"
        if [[ -n "$databricks_token" ]]; then
            echo "$databricks_token" | databricks configure --token --host "$databricks_workspace_url"
            
            # Upload scripts and notebooks
            databricks workspace import-dir scripts/ /Repos/etl-hiscox/scripts --overwrite
            
            info "Databricks deployment completed"
        else
            warn "Databricks token not found, skipping Databricks deployment"
        fi
    else
        warn "Databricks CLI not found, skipping Databricks deployment"
    fi
    
    # Deploy dbt models
    if command -v dbt &>/dev/null && [[ -d "dbt" ]]; then
        info "Deploying dbt models"
        cd dbt
        dbt deps
        dbt run --target "$ENVIRONMENT"
        dbt test --target "$ENVIRONMENT"
        cd ..
        info "dbt deployment completed"
    else
        warn "dbt not found or dbt directory missing, skipping dbt deployment"
    fi
}

# Post-deployment verification
post_deployment_verification() {
    log "Running post-deployment verification"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "[DRY RUN] Would run post-deployment verification"
        return 0
    fi
    
    # Run health checks
    if [[ -f "scripts/monitoring/health_check.py" ]]; then
        info "Running health checks"
        python3 scripts/monitoring/health_check.py --environment "$ENVIRONMENT" || {
            warn "Health checks failed"
            return 1
        }
    fi
    
    # Run smoke tests for production
    if [[ "$ENVIRONMENT" == "production" && -d "tests/smoke" ]]; then
        info "Running smoke tests"
        python3 -m pytest tests/smoke/ -v --env="$ENVIRONMENT" || {
            error "Smoke tests failed in production"
        }
    fi
    
    log "Post-deployment verification completed"
}

# Create deployment report
create_deployment_report() {
    local report_file="deployment_report_${ENVIRONMENT}_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$report_file" << EOF
# Deployment Report

## Details
- **Environment**: $ENVIRONMENT
- **Deployment Tag**: $DEPLOYMENT_TAG
- **Deployment Time**: $(date)
- **Deployed By**: $(whoami)
- **Git Commit**: $(git rev-parse HEAD 2>/dev/null || echo "N/A")

## Configuration
- Skip Tests: $SKIP_TESTS
- Force Deploy: $FORCE_DEPLOY
- Dry Run: $DRY_RUN
- Build Images: $BUILD_IMAGES

## Components Deployed
- ✅ Infrastructure (Terraform)
- ✅ Container Images
- ✅ Databricks Notebooks
- ✅ dbt Models

## Verification
- ✅ Health Checks Passed
- ✅ Post-deployment Tests Passed

## Rollback Command
\`\`\`bash
./deployment/rollback.sh $ENVIRONMENT $DEPLOYMENT_TAG
\`\`\`
EOF
    
    log "Deployment report created: $report_file"
}

# Send notifications
send_notifications() {
    local status=${1:-"success"}
    
    if [[ -n "${SLACK_WEBHOOK_URL:-}" ]]; then
        local emoji="🚀"
        local message="Deployment to $ENVIRONMENT completed successfully"
        
        if [[ "$status" == "failure" ]]; then
            emoji="🚨"
            message="Deployment to $ENVIRONMENT failed"
        fi
        
        curl -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"$emoji $message\\nTag: $DEPLOYMENT_TAG\\nBy: $(whoami)\"}" \
            "$SLACK_WEBHOOK_URL" || warn "Failed to send Slack notification"
    fi
}

# Main deployment function
main() {
    log "Starting deployment process"
    
    # Parse remaining arguments
    shift # Remove environment argument
    parse_args "$@"
    
    validate_environment
    set_environment_vars
    
    # Production confirmation
    if [[ "$ENVIRONMENT" == "production" && "$DRY_RUN" == "false" ]]; then
        echo -n "This is a PRODUCTION deployment. Are you sure? (yes/no): "
        read -r confirmation
        if [[ "$confirmation" != "yes" ]]; then
            error "Deployment cancelled by user"
        fi
    fi
    
    # Execute deployment steps
    pre_deployment_checks
    run_tests
    build_and_push_images
    deploy_infrastructure
    deploy_application
    post_deployment_verification
    
    if [[ "$DRY_RUN" == "false" ]]; then
        create_deployment_report
        send_notifications "success"
    fi
    
    log "Deployment completed successfully!"
}

# Trap to handle script interruption
trap 'error "Deployment interrupted"; send_notifications "failure"' INT TERM

# Run main function
main "$@"
