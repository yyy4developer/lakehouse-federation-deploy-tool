#!/usr/bin/env bash
# =============================================================================
# Lakehouse Federation Demo - One-click Deploy / Destroy
#
# Usage:
#   ./lakehouse_federation_demo_resource_deploy.sh            # Deploy
#   ./lakehouse_federation_demo_resource_deploy.sh --destroy  # Destroy
# =============================================================================

set -euo pipefail
cd "$(dirname "$0")"

export PATH="$HOME/.local/bin:$PATH"

if [[ "${1:-}" == "--destroy" ]]; then
    echo "================================================"
    echo "  Lakehouse Federation Demo - Destroy"
    echo "================================================"
else
    echo "================================================"
    echo "  Lakehouse Federation Demo - Deploy"
    echo "================================================"
fi
echo ""

# Check prerequisites
bash scripts/prerequisites.sh

# Run interactive deploy or destroy
uv run python scripts/deploy.py "$@"
