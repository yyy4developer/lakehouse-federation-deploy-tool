# =============================================================================
# Microsoft OneLake / Fabric (Catalog Federation)
# Uses REST API via az rest (no native Terraform Fabric provider)
# Skipped when fabric_lakehouse_id is pre-set (lakehouse already exists)
# =============================================================================

resource "null_resource" "fabric_lakehouse" {
  count = var.enable_onelake && var.fabric_lakehouse_id == "" ? 1 : 0

  triggers = {
    workspace_id = var.fabric_workspace_id
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      WORKSPACE_ID="${var.fabric_workspace_id}"
      LAKEHOUSE_NAME="lhf_demo_factory"

      echo "Creating Fabric Lakehouse '$LAKEHOUSE_NAME'..."
      LAKEHOUSE_RESPONSE=$(az rest --method POST \
        --url "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/lakehouses" \
        --headers "Content-Type=application/json" \
        --body "{\"displayName\": \"$LAKEHOUSE_NAME\", \"description\": \"Lakehouse Federation Demo - Factory data\"}" \
        --resource "https://api.fabric.microsoft.com" \
        2>&1 || true)

      echo "Lakehouse response: $LAKEHOUSE_RESPONSE"

      # Get lakehouse ID from create response
      LAKEHOUSE_ID=$(echo "$LAKEHOUSE_RESPONSE" | jq -r '.id // empty' 2>/dev/null)

      if [ -z "$LAKEHOUSE_ID" ]; then
        echo "Lakehouse may already exist, listing..."
        LAKEHOUSE_ID=$(az rest --method GET \
          --url "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/lakehouses" \
          --resource "https://api.fabric.microsoft.com" \
          | jq -r ".value[] | select(.displayName == \"$LAKEHOUSE_NAME\") | .id")
      fi

      if [ -z "$LAKEHOUSE_ID" ]; then
        echo "ERROR: Failed to create or find lakehouse"
        exit 1
      fi

      echo "Lakehouse ID: $LAKEHOUSE_ID"

      # Wait for lakehouse provisioning
      echo "Waiting 30s for lakehouse provisioning..."
      sleep 30

      # Upload Delta tables using deltalake Python library
      echo "Uploading Delta tables to OneLake..."
      cd ${path.module}
      uv run --project ${abspath(path.module)}/.. python scripts/onelake_upload.py "$WORKSPACE_ID" "$LAKEHOUSE_ID"

      echo "OneLake setup complete."
    EOT
  }
}
