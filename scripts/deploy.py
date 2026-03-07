#!/usr/bin/env python3
"""
Lakehouse Federation Demo - Interactive Deploy Script
"""

import json
import os
import random
import string
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
import questionary

console = Console()
PROJECT_ROOT = Path(__file__).parent.parent
TERRAFORM_DIR = PROJECT_ROOT / "terraform"

# Federation source definitions
SOURCES = {
    "glue":     {"label": "AWS Glue (Catalog Federation)",     "type": "catalog", "cloud_req": "aws"},
    "redshift": {"label": "Amazon Redshift (Query Federation)", "type": "query",   "cloud_req": None},
    "postgres": {"label": "PostgreSQL (Query Federation)",      "type": "query",   "cloud_req": None},
    "synapse":  {"label": "Azure Synapse (Query Federation)",   "type": "query",   "cloud_req": None},
    "bigquery": {"label": "Google BigQuery (Query Federation)", "type": "query",   "cloud_req": None},
    "onelake":  {"label": "OneLake / Fabric (Catalog Federation)", "type": "catalog", "cloud_req": "azure"},
}

# Source -> table definitions (for deploy_result.md)
SOURCE_TABLES = {
    "glue":     ["sensors", "machines", "quality_inspections"],
    "redshift": ["sensor_readings", "production_events", "quality_inspections"],
    "postgres": ["maintenance_logs", "work_orders"],
    "synapse":  ["shift_schedules", "energy_consumption"],
    "bigquery": ["downtime_records", "cost_allocation"],
    "onelake":  ["production_plans", "inventory_levels"],
}


def print_banner():
    console.print(Panel.fit(
        "[bold cyan]Lakehouse Federation Demo[/bold cyan]\n"
        "[dim]Multi-cloud federation deployment tool[/dim]",
        border_style="cyan",
    ))


def select_cloud() -> str:
    return questionary.select(
        "Databricks workspace のクラウドを選択:",
        choices=[
            questionary.Choice("AWS", value="aws"),
            questionary.Choice("Azure", value="azure"),
        ],
    ).ask()


def get_workspace_url() -> str:
    url = questionary.text(
        "Databricks workspace URL を入力 (例: https://fevm-xxx.cloud.databricks.com):",
    ).ask()

    if not url:
        console.print("\n[yellow]Workspace がない場合は FEVM で作成できます:[/yellow]")
        console.print("  Claude Code で [bold]/databricks-fe-vm-workspace-deployment[/bold] を実行")
        console.print("  AWS: aws_sandbox_serverless テンプレート")
        console.print("  Azure: azure_sandbox_classic テンプレート\n")

        url = questionary.text(
            "Workspace URL を入力 (必須):",
            validate=lambda x: len(x) > 0 or "URL は必須です",
        ).ask()

    return url.rstrip("/")


def setup_databricks_auth(workspace_url: str):
    """Setup Databricks OAuth U2M authentication via CLI."""
    console.print("\n[bold]Databricks OAuth 認証...[/bold]")
    console.print(f"  Workspace: {workspace_url}")

    result = subprocess.run(
        ["databricks", "auth", "token", "--host", workspace_url],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        console.print("  [green]✓[/green] OAuth 認証済み")
        return

    console.print("  [yellow]OAuth ログインが必要です。ブラウザが開きます。[/yellow]")
    result = subprocess.run(
        ["databricks", "auth", "login", "--host", workspace_url],
        timeout=120,
    )
    if result.returncode != 0:
        console.print("  [red]✗ OAuth 認証に失敗しました[/red]")
        sys.exit(1)
    console.print("  [green]✓[/green] OAuth 認証完了")


def select_sources(cloud: str) -> list[str]:
    choices = []
    for key, info in SOURCES.items():
        if info["cloud_req"] and info["cloud_req"] != cloud:
            continue
        choices.append(questionary.Choice(info["label"], value=key))

    selected = questionary.checkbox(
        "有効にする Federation ソースを選択:",
        choices=choices,
        validate=lambda x: len(x) > 0 or "少なくとも1つ選択してください",
    ).ask()

    return selected


def _random_suffix(length: int = 4) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def get_catalog_prefix() -> tuple[str, str]:
    default_query = "lhf_query"
    default_catalog = "lhf_catalog"

    query_prefix = questionary.text(
        "Query Federation カタログ prefix:",
        default=default_query,
    ).ask()

    catalog_prefix = questionary.text(
        "Catalog Federation カタログ prefix:",
        default=default_catalog,
    ).ask()

    # Add random suffix to avoid catalog name conflicts when using defaults
    if query_prefix == default_query or catalog_prefix == default_catalog:
        suffix = _random_suffix()
        console.print(f"  [dim]衝突回避のため乱数サフィックス '{suffix}' を追加[/dim]")
        if query_prefix == default_query:
            query_prefix = f"{default_query}_{suffix}"
        if catalog_prefix == default_catalog:
            catalog_prefix = f"{default_catalog}_{suffix}"

    return query_prefix, catalog_prefix


def check_cloud_auth(cloud: str, sources: list[str]) -> dict:
    """Check and guide cloud authentication. Returns auto-detected values."""
    console.print("\n[bold]認証チェック...[/bold]")
    creds_out = {}

    # AWS check
    needs_aws = cloud == "aws" or any(s in sources for s in ["glue", "redshift"])
    if needs_aws or ("postgres" in sources and cloud == "aws"):
        try:
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                console.print("  [green]✓[/green] AWS 認証済み")
            else:
                console.print("  [yellow]![/yellow] AWS 未認証 — SSO プロファイルを検索中...")
                profile = questionary.text(
                    "AWS SSO profile name (例: aws-sandbox-field-eng_databricks-sandbox-admin):",
                ).ask()
                if profile:
                    os.environ["AWS_PROFILE"] = profile
                    result = subprocess.run(
                        ["aws", "sso", "login", "--profile", profile],
                        timeout=120,
                    )
                    if result.returncode != 0:
                        console.print("  [red]✗[/red] AWS SSO login 失敗")
                        if not questionary.confirm("続行しますか?", default=False).ask():
                            sys.exit(1)
                    else:
                        console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
                else:
                    console.print("  [red]✗[/red] AWS 未認証 — [yellow]aws configure[/yellow] を実行してください")
                    if not questionary.confirm("続行しますか?", default=False).ask():
                        sys.exit(1)
        except FileNotFoundError:
            console.print("  [yellow]![/yellow] aws CLI が見つかりません")

    # Azure check
    if any(s in sources for s in ["synapse", "onelake"]) or (cloud == "azure" and "postgres" in sources):
        try:
            result = subprocess.run(
                ["az", "account", "show", "--query", "id", "-o", "tsv"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                sub_id = result.stdout.strip()
                console.print(f"  [green]✓[/green] Azure 認証済み (subscription: {sub_id})")
                creds_out["azure_subscription_id"] = sub_id
            else:
                console.print("  [red]✗[/red] Azure 未認証 — [yellow]az login[/yellow] を実行してください")
                if not questionary.confirm("続行しますか?", default=False).ask():
                    sys.exit(1)
        except FileNotFoundError:
            console.print("  [yellow]![/yellow] az CLI が見つかりません")

    # GCP check
    if "bigquery" in sources:
        try:
            result = subprocess.run(
                ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                console.print(f"  [green]✓[/green] GCP 認証済み ({result.stdout.strip()})")
            else:
                console.print("  [red]✗[/red] GCP 未認証 — [yellow]gcloud auth application-default login[/yellow] を実行してください")
                if not questionary.confirm("続行しますか?", default=False).ask():
                    sys.exit(1)
        except FileNotFoundError:
            console.print("  [yellow]![/yellow] gcloud CLI が見つかりません")

    console.print()
    return creds_out


def collect_credentials(cloud: str, sources: list[str], auto_creds: dict | None = None) -> dict:
    """Collect passwords and credentials per source."""
    creds = dict(auto_creds or {})

    if "redshift" in sources:
        creds["redshift_admin_password"] = questionary.password(
            "Redshift admin password (8+ chars, uppercase+lowercase+number):",
        ).ask()

    if "postgres" in sources:
        creds["postgres_admin_password"] = questionary.password(
            "PostgreSQL admin password (8+ chars):",
        ).ask()

    if "synapse" in sources:
        creds["synapse_admin_password"] = questionary.password(
            "Azure Synapse admin password:",
        ).ask()
        if "azure_subscription_id" not in creds:
            creds["azure_subscription_id"] = questionary.text(
                "Azure subscription ID:",
                validate=lambda x: len(x) > 0 or "必須です",
            ).ask()

    if "onelake" in sources:
        if "azure_subscription_id" not in creds:
            creds["azure_subscription_id"] = questionary.text(
                "Azure subscription ID:",
                validate=lambda x: len(x) > 0 or "必須です",
            ).ask()
        creds["fabric_workspace_id"] = questionary.text(
            "Fabric workspace ID (GUID):",
            validate=lambda x: len(x) > 0 or "必須です",
        ).ask()

    if cloud == "azure" and "postgres" in sources:
        if "azure_subscription_id" not in creds:
            creds["azure_subscription_id"] = questionary.text(
                "Azure subscription ID:",
                validate=lambda x: len(x) > 0 or "必須です",
            ).ask()

    if "bigquery" in sources:
        creds["gcp_project_id"] = questionary.text(
            "GCP project ID:",
            validate=lambda x: len(x) > 0 or "必須です",
        ).ask()
        creds["gcp_credentials_json"] = questionary.text(
            "GCP SA key JSON path (空白 = gcloud auth 使用):",
            default="",
        ).ask()
        if creds["gcp_credentials_json"]:
            key_path = Path(creds["gcp_credentials_json"]).expanduser()
            if key_path.exists():
                creds["gcp_credentials_json"] = key_path.read_text()
            else:
                console.print(f"[red]File not found: {key_path}[/red]")
                sys.exit(1)

    return creds


def generate_tfvars(
    cloud: str,
    workspace_url: str,
    sources: list[str],
    query_prefix: str,
    catalog_prefix: str,
    creds: dict,
):
    """Generate terraform.tfvars from collected config."""
    lines = [
        '# Auto-generated by deploy.py',
        f'project_prefix = "lhf-demo"',
        f'cloud          = "{cloud}"',
        "",
        "# Federation sources",
    ]

    for key in SOURCES:
        lines.append(f'enable_{key:10s} = {str(key in sources).lower()}')

    lines += [
        "",
        "# Catalog naming",
        f'catalog_prefix_query   = "{query_prefix}"',
        f'catalog_prefix_catalog = "{catalog_prefix}"',
        "",
        "# Databricks (OAuth U2M via CLI)",
        f'databricks_host = "{workspace_url}"',
        "",
        "# AWS",
        f'aws_region = "us-west-2"',
    ]

    # Add credentials
    for key, val in creds.items():
        if key == "gcp_credentials_json" and val:
            escaped = val.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            lines.append(f'{key} = "{escaped}"')
        else:
            lines.append(f'{key} = "{val}"')

    tfvars_path = TERRAFORM_DIR / "terraform.tfvars"
    tfvars_path.write_text("\n".join(lines) + "\n")
    console.print(f"\n[green]✓[/green] Generated {tfvars_path}")


def run_terraform():
    """Run terraform init, plan, apply."""
    console.print("\n[bold]Terraform deployment...[/bold]\n")

    for cmd_label, cmd in [
        ("terraform init",  ["terraform", "init"]),
        ("terraform plan",  ["terraform", "plan", "-out=tfplan"]),
        ("terraform apply", ["terraform", "apply", "tfplan"]),
    ]:
        console.print(f"[cyan]▶ {cmd_label}[/cyan]")
        result = subprocess.run(cmd, cwd=TERRAFORM_DIR)
        if result.returncode != 0:
            console.print(f"[red]✗ {cmd_label} failed (exit {result.returncode})[/red]")
            sys.exit(1)
        console.print(f"[green]✓ {cmd_label}[/green]\n")


def deploy_dab(workspace_url: str):
    """Deploy notebooks via DAB (uses OAuth from CLI profile)."""
    console.print("[bold]DAB deployment...[/bold]\n")

    env = os.environ.copy()
    env["DATABRICKS_HOST"] = workspace_url

    result = subprocess.run(
        ["databricks", "bundle", "deploy", "--target", "dev"],
        cwd=PROJECT_ROOT,
        env=env,
    )

    if result.returncode == 0:
        console.print("[green]✓ DAB deploy complete[/green]\n")
    else:
        console.print("[yellow]! DAB deploy failed (non-critical)[/yellow]\n")


def get_terraform_outputs() -> dict:
    """Fetch terraform output as JSON."""
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=TERRAFORM_DIR,
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return {}
    try:
        raw = json.loads(result.stdout)
        return {k: v.get("value") for k, v in raw.items()}
    except (json.JSONDecodeError, AttributeError):
        return {}


def generate_deploy_result(
    cloud: str,
    workspace_url: str,
    sources: list[str],
    query_prefix: str,
    catalog_prefix: str,
):
    """Generate deploy_result.md with access links and resource tree."""
    outputs = get_terraform_outputs()
    catalogs = outputs.get("databricks_catalogs", {})
    db_names = outputs.get("database_names", {})
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        "# Lakehouse Federation Demo - Deploy Result",
        "",
        f"**Deployed at**: {now}",
        f"**Cloud**: {cloud}",
        f"**Workspace**: {workspace_url}",
        "",
        "---",
        "",
        "## Access Links",
        "",
        f"| Resource | URL |",
        f"|----------|-----|",
        f"| Databricks Workspace | {workspace_url} |",
        f"| Demo Notebook | {workspace_url}/#workspace/Shared/lakehouse_federation_demo/notebooks/federation_demo |",
    ]

    if "glue" in sources:
        lines.append(f"| AWS Glue Console | https://{outputs.get('s3_bucket_name', 'us-west-2')}.console.aws.amazon.com/glue/ |")
    if "redshift" in sources and outputs.get("redshift_endpoint"):
        lines.append(f"| Redshift Endpoint | `{outputs['redshift_endpoint']}:5439` |")
    if "postgres" in sources and outputs.get("postgres_endpoint"):
        lines.append(f"| PostgreSQL Endpoint | `{outputs['postgres_endpoint']}:5432` |")
    if "synapse" in sources and outputs.get("synapse_endpoint"):
        lines.append(f"| Synapse Endpoint | `{outputs['synapse_endpoint']}:1433` |")
    if "bigquery" in sources:
        lines.append(f"| BigQuery Console | https://console.cloud.google.com/bigquery |")

    lines += [
        "",
        "---",
        "",
        "## Deployed Resource Tree",
        "",
        "```",
        "Unity Catalog",
    ]

    # Catalog Federation sources
    for src in ["glue", "onelake"]:
        if src not in sources:
            continue
        cat_name = catalogs.get(src, f"{catalog_prefix}_{src}")
        db = db_names.get(src, "default")
        prefix = "catalog" if src != "onelake" else "catalog"
        lines.append(f"├── {cat_name}  (Catalog Federation: {SOURCES[src]['label']})")
        tables = SOURCE_TABLES.get(src, [])
        lines.append(f"│   └── {db}")
        for i, t in enumerate(tables):
            connector = "├" if i < len(tables) - 1 else "└"
            lines.append(f"│       {connector}── {t}")

    # Query Federation sources
    for src in ["redshift", "postgres", "synapse", "bigquery"]:
        if src not in sources:
            continue
        cat_name = catalogs.get(src, f"{query_prefix}_{src}")
        db = db_names.get(src, "unknown")
        schema = "dbo" if src == "synapse" else (db_names.get(src, "unknown") if src == "bigquery" else "public")
        lines.append(f"├── {cat_name}  (Query Federation: {SOURCES[src]['label']})")
        lines.append(f"│   └── {schema}")
        tables = SOURCE_TABLES.get(src, [])
        for i, t in enumerate(tables):
            connector = "├" if i < len(tables) - 1 else "└"
            lines.append(f"│       {connector}── {t}")

    lines += [
        "```",
        "",
        "---",
        "",
        "## Databricks Catalogs",
        "",
        "| Source | Catalog Name | Database/Schema | Tables |",
        "|--------|-------------|-----------------|--------|",
    ]

    for src in sources:
        cat = catalogs.get(src, "N/A")
        db = db_names.get(src, "N/A")
        tables = ", ".join(SOURCE_TABLES.get(src, []))
        lines.append(f"| {SOURCES[src]['label']} | `{cat}` | `{db}` | {tables} |")

    lines += [""]

    result_path = PROJECT_ROOT / "deploy_result.md"
    result_path.write_text("\n".join(lines) + "\n")
    console.print(f"[green]✓[/green] Generated {result_path}")
    return result_path


# (table_name, fqn_template, expected_rows)
SOURCE_TEST_QUERIES = {
    "glue": [
        ("sensors", "{catalog_prefix}_glue.{db_prefix}_factory_master.sensors", 20),
        ("machines", "{catalog_prefix}_glue.{db_prefix}_factory_master.machines", 10),
        ("quality_inspections", "{catalog_prefix}_glue.{db_prefix}_factory_master.quality_inspections", 50),
    ],
    "redshift": [
        ("sensor_readings", "{query_prefix}_redshift.public.sensor_readings", 100),
        ("production_events", "{query_prefix}_redshift.public.production_events", 30),
        ("quality_inspections", "{query_prefix}_redshift.public.quality_inspections", 40),
    ],
    "postgres": [
        ("maintenance_logs", "{query_prefix}_postgres.public.maintenance_logs", 30),
        ("work_orders", "{query_prefix}_postgres.public.work_orders", 25),
    ],
    "synapse": [
        ("shift_schedules", "{query_prefix}_synapse.dbo.shift_schedules", 40),
        ("energy_consumption", "{query_prefix}_synapse.dbo.energy_consumption", 50),
    ],
    "bigquery": [
        ("downtime_records", "{query_prefix}_bigquery.{db_prefix}_factory.downtime_records", 35),
        ("cost_allocation", "{query_prefix}_bigquery.{db_prefix}_factory.cost_allocation", 30),
    ],
    "onelake": [
        ("production_plans", "{catalog_prefix}_onelake.default.production_plans", 20),
        ("inventory_levels", "{catalog_prefix}_onelake.default.inventory_levels", 30),
    ],
}


def _get_databricks_token(workspace_url: str) -> str | None:
    """Get OAuth token via databricks CLI."""
    try:
        result = subprocess.run(
            ["databricks", "auth", "token", "--host", workspace_url],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return json.loads(result.stdout).get("access_token")
    except Exception:
        pass
    return None


def _get_warehouse_id(workspace_url: str, token: str) -> str | None:
    """Find a running SQL warehouse."""
    try:
        result = subprocess.run(
            ["curl", "-s", f"{workspace_url}/api/2.0/sql/warehouses",
             "-H", f"Authorization: Bearer {token}"],
            capture_output=True, text=True, timeout=15,
        )
        warehouses = json.loads(result.stdout).get("warehouses", [])
        for w in warehouses:
            if w.get("state") == "RUNNING":
                return w["id"]
        if warehouses:
            return warehouses[0]["id"]
    except Exception:
        pass
    return None


def _execute_sql(workspace_url: str, token: str, warehouse_id: str, sql: str) -> dict:
    """Execute SQL via Databricks SQL Statements API."""
    result = subprocess.run(
        ["curl", "-s", "-X", "POST",
         f"{workspace_url}/api/2.0/sql/statements",
         "-H", f"Authorization: Bearer {token}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps({
             "statement": sql,
             "warehouse_id": warehouse_id,
             "wait_timeout": "50s",
             "on_wait_timeout": "CANCEL",
         })],
        capture_output=True, text=True, timeout=90,
    )
    return json.loads(result.stdout)


def run_connectivity_test(
    workspace_url: str,
    sources: list[str],
    query_prefix: str,
    catalog_prefix: str,
) -> bool:
    """Run connectivity tests against all deployed sources via Databricks SQL."""
    outputs = get_terraform_outputs()
    db_names = outputs.get("database_names", {})
    # Derive db_prefix from any available database name
    sample_db = next(iter(db_names.values()), "lhf_demo_factory")
    db_prefix = sample_db.rsplit("_factory", 1)[0]

    console.print("\n")
    console.print(Panel("[bold cyan]Connectivity Test[/bold cyan]", border_style="cyan"))

    token = _get_databricks_token(workspace_url)
    if not token:
        console.print("[red]Could not get Databricks token. Skipping tests.[/red]")
        return False

    warehouse_id = _get_warehouse_id(workspace_url, token)
    if not warehouse_id:
        console.print("[red]No SQL warehouse found. Skipping tests.[/red]")
        return False

    console.print(f"  Using warehouse: {warehouse_id}\n")

    table = Table(title="Federation Source Tests")
    table.add_column("Source", style="cyan")
    table.add_column("Table")
    table.add_column("Expected", justify="right")
    table.add_column("Actual", justify="right")
    table.add_column("Status")

    all_passed = True

    for src in sources:
        queries = SOURCE_TEST_QUERIES.get(src, [])
        for tbl_name, fqn_template, expected in queries:
            fqn = fqn_template.format(
                query_prefix=query_prefix,
                catalog_prefix=catalog_prefix,
                db_prefix=db_prefix,
            )
            sql = f"SELECT count(*) AS cnt FROM {fqn}"

            try:
                resp = _execute_sql(workspace_url, token, warehouse_id, sql)
                state = resp.get("status", {}).get("state", "UNKNOWN")

                if state == "SUCCEEDED":
                    data = resp.get("result", {}).get("data_array", [])
                    actual = int(data[0][0]) if data else 0
                    if actual == expected:
                        table.add_row(src, tbl_name, str(expected), str(actual), "[green]PASS[/green]")
                    else:
                        table.add_row(src, tbl_name, str(expected), str(actual), "[yellow]MISMATCH[/yellow]")
                        all_passed = False
                else:
                    error_msg = resp.get("status", {}).get("error", {}).get("message", state)
                    table.add_row(src, tbl_name, str(expected), error_msg[:30], "[red]FAIL[/red]")
                    all_passed = False

            except Exception as e:
                table.add_row(src, tbl_name, str(expected), str(e)[:30], "[red]ERROR[/red]")
                all_passed = False

    console.print(table)

    if all_passed:
        console.print("\n[bold green]All connectivity tests passed![/bold green]")
    else:
        console.print("\n[bold red]Some tests failed. Check the table above.[/bold red]")

    return all_passed


def print_summary(
    cloud: str,
    workspace_url: str,
    sources: list[str],
    query_prefix: str,
    catalog_prefix: str,
):
    """Print deployed resource summary."""
    console.print("\n")
    console.print(Panel("[bold green]Deploy Complete![/bold green]", border_style="green"))

    table = Table(title="Deployed Resources")
    table.add_column("Source", style="cyan")
    table.add_column("Type")
    table.add_column("Status", style="green")

    for s in sources:
        info = SOURCES[s]
        table.add_row(info["label"], info["type"], "✓ deployed")

    console.print(table)

    console.print(f"\n[bold]Databricks Workspace:[/bold] {workspace_url}")
    console.print(f"[bold]Demo Notebook:[/bold] {workspace_url}/#workspace/Shared/lakehouse_federation_demo/notebooks/federation_demo")

    # Run connectivity tests
    run_connectivity_test(workspace_url, sources, query_prefix, catalog_prefix)

    # Generate deploy_result.md
    result_path = generate_deploy_result(cloud, workspace_url, sources, query_prefix, catalog_prefix)
    console.print(f"\n[bold]Deploy Result:[/bold] {result_path}")

    # Print terraform outputs
    console.print("\n[bold]Terraform Outputs:[/bold]")
    subprocess.run(["terraform", "output"], cwd=TERRAFORM_DIR)
    console.print()


def main():
    print_banner()

    cloud = select_cloud()
    workspace_url = get_workspace_url()
    sources = select_sources(cloud)
    query_prefix, catalog_prefix = get_catalog_prefix()
    auto_creds = check_cloud_auth(cloud, sources)
    setup_databricks_auth(workspace_url)
    creds = collect_credentials(cloud, sources, auto_creds)

    # Confirm
    console.print("\n[bold]Configuration Summary:[/bold]")
    console.print(f"  Cloud: {cloud}")
    console.print(f"  Workspace: {workspace_url}")
    console.print(f"  Sources: {', '.join(sources)}")
    console.print(f"  Prefixes: {query_prefix} / {catalog_prefix}")

    if not questionary.confirm("\nデプロイを開始しますか?", default=True).ask():
        console.print("[yellow]Cancelled.[/yellow]")
        sys.exit(0)

    generate_tfvars(cloud, workspace_url, sources, query_prefix, catalog_prefix, creds)
    run_terraform()
    deploy_dab(workspace_url)
    print_summary(cloud, workspace_url, sources, query_prefix, catalog_prefix)


if __name__ == "__main__":
    main()
