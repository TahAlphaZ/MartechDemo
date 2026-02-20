#!/bin/bash
# ============================================================
# MarTech Fabric Pipeline - Infrastructure Deployment Script
# Usage: ./deploy.sh <environment> [db_type]
# Example: ./deploy.sh dev postgres
#          ./deploy.sh prod sqlserver
# ============================================================

set -euo pipefail

ENVIRONMENT="${1:-dev}"
DB_TYPE="${2:-postgres}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARM_DIR="${SCRIPT_DIR}/../arm-templates"
PROJECT_NAME="martech"
RESOURCE_GROUP="rg-${PROJECT_NAME}-${ENVIRONMENT}"
LOCATION="eastus2"

echo "============================================"
echo "  MarTech Pipeline Deployment"
echo "  Environment: ${ENVIRONMENT}"
echo "  DB Type:     ${DB_TYPE}"
echo "  RG:          ${RESOURCE_GROUP}"
echo "============================================"

# Step 1: Validate ARM templates
echo "[1/4] Validating ARM templates..."
az deployment group validate \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file "${ARM_DIR}/main.json" \
  --parameters "${ARM_DIR}/parameters/${ENVIRONMENT}.parameters.json" \
  --parameters dbConnectorType="${DB_TYPE}"

echo "[2/4] Running pre-deploy tests..."
python -m pytest tests/unit/ -v --tb=short || {
  echo "ERROR: Unit tests failed. Aborting deployment."
  exit 1
}

# Step 3: Deploy
echo "[3/4] Deploying infrastructure..."
az deployment group create \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file "${ARM_DIR}/main.json" \
  --parameters "${ARM_DIR}/parameters/${ENVIRONMENT}.parameters.json" \
  --parameters dbConnectorType="${DB_TYPE}" \
  --name "deploy-${ENVIRONMENT}-$(date +%Y%m%d%H%M%S)"

# Step 4: Post-deploy validation
echo "[4/4] Running post-deploy validation..."
python -m pytest tests/integration/ -v --tb=short -k "smoke" || {
  echo "WARNING: Post-deploy smoke tests had failures."
}

echo "============================================"
echo "  Deployment complete!"
echo "============================================"
