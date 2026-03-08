#!/usr/bin/env python3
"""Upload demo Delta tables to OneLake (Fabric Lakehouse).

Usage:
  python onelake_upload.py <workspace_id> <lakehouse_id>

Requires: az login (Azure AD token used for OneLake auth)
"""
import json
import subprocess
import sys
from datetime import date, timedelta

import pandas as pd
from deltalake import write_deltalake


def get_azure_token() -> str:
    """Get Azure AD bearer token for OneLake (storage.azure.com resource)."""
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://storage.azure.com/", "-o", "json"],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)["accessToken"]


def make_production_plans() -> pd.DataFrame:
    """Generate production_plans table (20 rows)."""
    base = date(2024, 1, 8)
    rows = []
    products = ["Widget-A", "Widget-B", "Gear-X", "Gear-Y", "Shaft-S1"]
    for i in range(1, 21):
        machine_id = ((i - 1) % 10) + 1
        product = products[(i - 1) % len(products)]
        plan_date = base + timedelta(days=(i - 1) * 2)
        target_qty = 100 + (i * 15)
        actual_qty = target_qty - ((i * 7) % 30)
        rows.append({
            "plan_id": i,
            "machine_id": machine_id,
            "product_name": product,
            "plan_date": plan_date.isoformat(),
            "target_quantity": target_qty,
            "actual_quantity": actual_qty,
            "status": "completed" if i <= 15 else "in_progress",
        })
    return pd.DataFrame(rows)


def make_inventory_levels() -> pd.DataFrame:
    """Generate inventory_levels table (30 rows)."""
    base = date(2024, 1, 10)
    materials = ["Steel-Rod", "Copper-Wire", "Aluminum-Sheet", "Rubber-Seal", "Bearing-6205", "Lubricant-G3"]
    rows = []
    for i in range(1, 31):
        machine_id = ((i - 1) % 10) + 1
        material = materials[(i - 1) % len(materials)]
        record_date = base + timedelta(days=(i - 1))
        qty_on_hand = 500 - (i * 12)
        reorder_point = 100
        rows.append({
            "inventory_id": i,
            "machine_id": machine_id,
            "material_name": material,
            "record_date": record_date.isoformat(),
            "quantity_on_hand": max(qty_on_hand, 50),
            "reorder_point": reorder_point,
            "unit": "pcs" if "Bearing" in material or "Seal" in material else "kg",
            "warehouse_location": f"WH-{chr(65 + (i % 4))}",
        })
    return pd.DataFrame(rows)


def upload_table(workspace_id: str, lakehouse_id: str, table_name: str, df: pd.DataFrame, token: str):
    """Write a DataFrame as a Delta table to OneLake."""
    uri = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"
    storage_options = {
        "bearer_token": token,
        "use_fabric_endpoint": "true",
    }
    print(f"  Writing {table_name} ({len(df)} rows) to OneLake...")
    write_deltalake(uri, df, mode="overwrite", storage_options=storage_options)
    print(f"  {table_name} uploaded successfully.")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <workspace_id> <lakehouse_id>", file=sys.stderr)
        sys.exit(1)

    workspace_id = sys.argv[1]
    lakehouse_id = sys.argv[2]

    print("Getting Azure AD token for OneLake...")
    token = get_azure_token()

    print(f"Uploading tables to Lakehouse {lakehouse_id} in workspace {workspace_id}...")
    upload_table(workspace_id, lakehouse_id, "production_plans", make_production_plans(), token)
    upload_table(workspace_id, lakehouse_id, "inventory_levels", make_inventory_levels(), token)

    print("OneLake data upload complete.")


if __name__ == "__main__":
    main()
