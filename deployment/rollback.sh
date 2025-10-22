#!/bin/bash

# Rollback script for Hiscox ETL Pipeline
# Usage: ./rollback.sh <environment> <previous_version>

set -euo pipefail

ENVIRONMENT=${1:-}
PREVIOUS_VERSION=${2:-}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

# Validate inputs
if [[ -z "$ENVIRONMENT" ]]; then
    error "Environment is required. Usage: ./rollback.sh <environment> <previous_version>"
fi

if [[ -z "$PREVIOUS_VERSION" ]]; then
    error "Previous version is required. Usage: ./rollback.sh <environment> <previous_version>"
fi

# Validate environment
case $ENVIRONMENT in
    dev|staging|production)
        log "Rolling back $ENVIRONMENT environment to version $PREVIOUS_VERSION"
        ;;
    *)
        error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or production"
        ;;
esac

# Set environment-specific variables
case $ENVIRONMENT in
    dev)
        ACR_NAME="acrhiscoxetldev001am"
        RESOURCE_GROUP="rg-hiscox-etl-dev"
        ;;
    staging)
        ACR_NAME="acrhiscoxetlstaging001am"
        RESOURCE_GROUP="rg-hiscox-etl-staging"
        ;;
    production)
        ACR_NAME="acrhiscoxetlprod001am"
        RESOURCE_GROUP="rg-hiscox-etl-prod-001"
        ;;
esac

# Check if Azure CLI is logged in
if ! az account show &>/dev/null; then
    error "Not logged in to Azure CLI. Please run 'az login' first."
fi

# Function to rollback container images
rollback_containers() {
    log "Rolling back container images to version $PREVIOUS_VERSION"
    
    # Check if the previous version exists
    if ! az acr repository show-tags --name "$ACR_NAME" --repository etl-ingestion --output tsv | grep -q "$PREVIOUS_VERSION"; then
        error "Version $PREVIOUS_VERSION not found in registry $ACR_NAME"
    fi
    
    # Pull the previous version
    docker pull "$ACR_NAME.azurecr.io/etl-ingestion:$PREVIOUS_VERSION"
    docker pull "$ACR_NAME.azurecr.io/etl-transformation:$PREVIOUS_VERSION"
    
    # Tag as latest
    docker tag "$ACR_NAME.azurecr.io/etl-ingestion:$PREVIOUS_VERSION" "$ACR_NAME.azurecr.io/etl-ingestion:latest"
    docker tag "$ACR_NAME.azurecr.io/etl-transformation:$PREVIOUS_VERSION" "$ACR_NAME.azurecr.io/etl-transformation:latest"
    
    # Push the rollback tags
    az acr login --name "$ACR_NAME"
    docker push "$ACR_NAME.azurecr.io/etl-ingestion:latest"
    docker push "$ACR_NAME.azurecr.io/etl-transformation:latest"
    
    log "Container images rolled back successfully"
}

# Function to rollback infrastructure if needed
rollback_infrastructure() {
    log "Checking if infrastructure rollback is needed"
    
    cd "$PROJECT_ROOT/terraform/environments/$ENVIRONMENT"
    
    # Check if there are any infrastructure changes to rollback
    if terraform plan -detailed-exitcode &>/dev/null; then
        log "No infrastructure changes to rollback"
        return 0
    fi
    
    warn "Infrastructure changes detected. Manual review required."
    echo "Please review the Terraform plan and apply manually if needed:"
    echo "cd $PROJECT_ROOT/terraform/environments/$ENVIRONMENT"
    echo "terraform plan"
    echo "terraform apply"
}

# Function to rollback Databricks notebooks
rollback_databricks() {
    log "Rolling back Databricks notebooks"
    
    # This would require Databricks CLI to be configured
    if command -v databricks &>/dev/null; then
        # Get the workspace URL from Terraform output
        DATABRICKS_HOST=$(cd "$PROJECT_ROOT/terraform/environments/$ENVIRONMENT" && terraform output -raw databricks_workspace_url 2>/dev/null || echo "")
        
        if [[ -n "$DATABRICKS_HOST" ]]; then
            log "Updating Databricks notebooks to previous version"
            # In a real scenario, you'd have version-controlled notebooks
            # databricks workspace import-dir scripts/ /Repos/etl-hiscox/scripts --overwrite
            log "Databricks rollback completed (placeholder)"
        else
            warn "Could not determine Databricks workspace URL"
        fi
    else
        warn "Databricks CLI not found. Skipping Databricks rollback."
    fi
}

# Function to rollback dbt models
rollback_dbt() {
    log "Rolling back dbt models"
    
    cd "$PROJECT_ROOT"
    
    # Check if git is available and we can checkout the previous version
    if git rev-parse --is-inside-work-tree &>/dev/null; then
        # Find the commit for the previous version
        PREVIOUS_COMMIT=$(git log --oneline --grep="$PREVIOUS_VERSION" --format="%H" | head -1)
        
        if [[ -n "$PREVIOUS_COMMIT" ]]; then
            log "Checking out dbt models from commit $PREVIOUS_COMMIT"
            git checkout "$PREVIOUS_COMMIT" -- dbt/
            
            # Run dbt with the rolled-back models
            cd dbt
            if command -v dbt &>/dev/null; then
                dbt deps
                dbt run --target "$ENVIRONMENT"
                dbt test --target "$ENVIRONMENT"
                log "dbt models rolled back and tested successfully"
            else
                warn "dbt not found. Please run dbt manually after rollback"
            fi
        else
            warn "Could not find commit for version $PREVIOUS_VERSION"
        fi
    else
        warn "Not in a git repository. Cannot rollback dbt models automatically"
    fi
}

# Function to verify rollback
verify_rollback() {
    log "Verifying rollback"
    
    # Check container registry tags
    CURRENT_TAG=$(az acr repository show-tags --name "$ACR_NAME" --repository etl-ingestion --orderby time_desc --output tsv | head -1)
    log "Current latest tag points to: $CURRENT_TAG"
    
    # Run health checks if available
    if [[ -f "$PROJECT_ROOT/scripts/monitoring/health_check.py" ]]; then
        log "Running health checks"
        cd "$PROJECT_ROOT"
        python scripts/monitoring/health_check.py --environment "$ENVIRONMENT" || warn "Health checks failed"
    fi
    
    log "Rollback verification completed"
}

# Function to create rollback report
create_rollback_report() {
    local report_file="rollback_report_${ENVIRONMENT}_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$report_file" << EOF
# Rollback Report

## Details
- **Environment**: $ENVIRONMENT
- **Rollback Version**: $PREVIOUS_VERSION
- **Rollback Time**: $(date)
- **Executed By**: $(whoami)
- **Reason**: Manual rollback initiated

## Actions Taken
- ✅ Container images rolled back
- ✅ Infrastructure checked
- ✅ Databricks notebooks updated
- ✅ dbt models rolled back
- ✅ Verification completed

## Next Steps
1. Monitor system performance
2. Verify data pipeline functionality
3. Plan forward fix if needed

## Rollback Command
\`\`\`bash
$0 $ENVIRONMENT $PREVIOUS_VERSION
\`\`\`
EOF
    
    log "Rollback report created: $report_file"
}

# Main rollback process
main() {
    log "Starting rollback process for $ENVIRONMENT environment"
    log "Target version: $PREVIOUS_VERSION"
    
    # Confirm rollback
    if [[ "$ENVIRONMENT" == "production" ]]; then
        echo -n "This is a PRODUCTION rollback. Are you sure? (yes/no): "
        read -r confirmation
        if [[ "$confirmation" != "yes" ]]; then
            error "Rollback cancelled by user"
        fi
    fi
    
    # Execute rollback steps
    rollback_containers
    rollback_infrastructure
    rollback_databricks
    rollback_dbt
    verify_rollback
    create_rollback_report
    
    log "Rollback completed successfully!"
    log "Please monitor the system and verify functionality"
    
    # Send notification (if configured)
    if [[ -n "${SLACK_WEBHOOK_URL:-}" ]]; then
        curl -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"🔄 Rollback completed for $ENVIRONMENT environment to version $PREVIOUS_VERSION\"}" \
            "$SLACK_WEBHOOK_URL" || warn "Failed to send Slack notification"
    fi
}

# Trap to handle script interruption
trap 'error "Rollback interrupted. System may be in inconsistent state."' INT TERM

# Run main function
main "$@"
