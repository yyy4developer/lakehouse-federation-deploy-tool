#!/usr/bin/env python3
"""
Lakehouse Federation Demo - Interactive Deploy Script
"""

import json
import os
import random
import re
import string
import subprocess
import sys
import time
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
    "postgres": ["machines", "maintenance_logs", "work_orders"],
    "synapse":  ["shift_schedules", "energy_consumption"],
    "bigquery": ["downtime_records", "cost_allocation"],
    "onelake":  ["production_plans", "inventory_levels"],
}


NOTEBOOK_TEMPLATE = PROJECT_ROOT / "notebooks" / "federation_demo_template.sql"
NOTEBOOK_OUTPUT = PROJECT_ROOT / "notebooks" / "federation_demo.sql"
NOTEBOOK_WIDGET_OUTPUT = PROJECT_ROOT / "notebooks" / "federation_demo_interactive.sql"
DEPLOY_STATE_FILE = PROJECT_ROOT / ".deploy_state.json"

# Mapping of source -> section markers in the notebook template
# Each source maps to section header patterns that should be included
SOURCE_SECTIONS = {
    "glue":     ["1.1 Catalog Federation: AWS Glue"],
    "redshift": ["1.2 Query Federation: Amazon Redshift"],
    "postgres": ["1.3 Query Federation: PostgreSQL"],
    "synapse":  ["1.4 Query Federation: Azure Synapse"],
    "bigquery": ["1.5 Query Federation: Google BigQuery"],
    "onelake":  ["1.6 Catalog Federation: Microsoft OneLake"],
}

# Chapter 2 cross-source JOIN section markers
CH2_SOURCE_MARKERS = {
    "postgres": "PostgreSQL: 保守履歴の統合",
    "synapse": "Synapse: シフト・エネルギーの統合",
    "bigquery": "BigQuery: 稼働停止・コスト分析",
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

    # Clean URL: remove path/query (e.g. /browse?o=123) and trailing slash
    from urllib.parse import urlparse
    parsed = urlparse(url.strip())
    clean = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    if clean != url.strip().rstrip("/"):
        console.print(f"  [dim]URL をクリーンアップ: {clean}[/dim]")
    return clean


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


def _create_gcp_sa_key(project_id: str) -> str:
    """Auto-create GCP service account and key JSON for BigQuery."""
    sa_name = "lhf-demo"
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    key_path = Path.home() / f"gcp-sa-key-{project_id}.json"

    # Check if SA already exists
    check = subprocess.run(
        ["gcloud", "iam", "service-accounts", "describe", sa_email, f"--project={project_id}"],
        capture_output=True, text=True, timeout=15,
    )
    if check.returncode != 0:
        console.print(f"  SA 作成中: {sa_email}")
        result = subprocess.run(
            ["gcloud", "iam", "service-accounts", "create", sa_name,
             f"--project={project_id}", "--display-name=LHF Demo"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            console.print(f"[red]SA 作成失敗: {result.stderr}[/red]")
            sys.exit(1)
    else:
        console.print(f"  SA 既存: {sa_email}")

    # Grant BigQuery admin role (idempotent)
    console.print("  BigQuery 権限付与中...")
    subprocess.run(
        ["gcloud", "projects", "add-iam-policy-binding", project_id,
         f"--member=serviceAccount:{sa_email}",
         "--role=roles/bigquery.admin",
         "--condition=None", "--quiet"],
        capture_output=True, text=True, timeout=30,
    )

    # Check if key file already exists and is valid
    if key_path.exists():
        try:
            key_data = json.loads(key_path.read_text())
            if key_data.get("project_id") == project_id:
                console.print(f"  [green]✓[/green] 既存 key 使用: {key_path}")
                return key_path.read_text()
        except (json.JSONDecodeError, KeyError):
            pass

    # Generate new key
    console.print(f"  key JSON 生成中: {key_path}")
    result = subprocess.run(
        ["gcloud", "iam", "service-accounts", "keys", "create", str(key_path),
         f"--iam-account={sa_email}"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        console.print(f"[red]key 生成失敗: {result.stderr}[/red]")
        sys.exit(1)

    console.print(f"  [green]✓[/green] SA key 自動作成完了: {key_path}")
    return key_path.read_text()


def get_catalog_prefix() -> tuple[str, str, str]:
    suffix = _random_suffix()
    default_prefix = f"lhf_{suffix}"

    console.print(f"[dim]  自動生成 prefix: {default_prefix} (変更可)[/dim]")
    prefix = questionary.text(
        "カタログ名の共通 prefix:",
        default=default_prefix,
    ).ask()

    query_prefix = f"{prefix}_query"
    catalog_prefix = f"{prefix}_catalog"
    analysis_catalog = f"{prefix}_union_dbx"

    console.print(f"  Query Federation:   {query_prefix}_*")
    console.print(f"  Catalog Federation: {catalog_prefix}_*")
    console.print(f"  分析結果カタログ:   {analysis_catalog}")

    return query_prefix, catalog_prefix, analysis_catalog


def _aws_auth_ok() -> bool:
    """Check if current AWS credentials are valid."""
    try:
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0 and "ExpiredToken" not in result.stderr
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def check_cloud_auth(cloud: str, sources: list[str]) -> dict:
    """Check and guide cloud authentication. Returns auto-detected values."""
    console.print("\n[bold]認証チェック...[/bold]")
    creds_out = {}

    # AWS check
    needs_aws = cloud == "aws" or any(s in sources for s in ["glue", "redshift"])
    if needs_aws or ("postgres" in sources and cloud == "aws"):
        try:
            if _aws_auth_ok():
                profile = os.environ.get("AWS_PROFILE", "default")
                console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
            else:
                # Try to find SSO profiles
                console.print("  [yellow]![/yellow] AWS 未認証またはトークン期限切れ")
                try:
                    profiles_result = subprocess.run(
                        ["aws", "configure", "list-profiles"],
                        capture_output=True, text=True, timeout=5,
                    )
                    all_profiles = [p.strip() for p in profiles_result.stdout.splitlines()]
                    # Prefer sandbox-field-eng sandbox-admin
                    sso_profiles = [p for p in all_profiles if "sandbox-field-eng" in p and "sandbox-admin" in p]
                    if not sso_profiles:
                        sso_profiles = [p for p in all_profiles if "sandbox-field-eng" in p]
                    if not sso_profiles:
                        sso_profiles = [p for p in all_profiles if "sandbox" in p.lower()]
                except Exception:
                    sso_profiles = []

                default_profile = sso_profiles[0] if sso_profiles else ""
                profile = questionary.text(
                    "AWS SSO profile name:",
                    default=default_profile,
                ).ask()

                if profile:
                    os.environ["AWS_PROFILE"] = profile
                    # Try existing token first
                    if _aws_auth_ok():
                        console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
                    else:
                        console.print(f"  [yellow]SSO ログインが必要です...[/yellow]")
                        result = subprocess.run(
                            ["aws", "sso", "login", "--profile", profile],
                            timeout=120,
                        )
                        if result.returncode != 0 or not _aws_auth_ok():
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
                # Auto-detect project ID
                proj = subprocess.run(
                    ["gcloud", "config", "get-value", "project"],
                    capture_output=True, text=True, timeout=10,
                )
                if proj.returncode == 0 and proj.stdout.strip():
                    creds_out["gcp_project_id"] = proj.stdout.strip()
                    console.print(f"  [green]✓[/green] GCP project: {proj.stdout.strip()}")
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

    default_pw = f"LhfDemo#{_random_suffix(6)}"

    if "redshift" in sources:
        console.print(f"  [dim]デフォルト password: {default_pw}[/dim]")
        pw = questionary.password(
            "Redshift admin password (空白 Enter = 自動生成):",
        ).ask()
        creds["redshift_admin_password"] = pw if pw else default_pw

    if "postgres" in sources:
        if "redshift_admin_password" not in creds:
            console.print(f"  [dim]デフォルト password: {default_pw}[/dim]")
        pw = questionary.password(
            "PostgreSQL admin password (空白 Enter = 自動生成):",
        ).ask()
        creds["postgres_admin_password"] = pw if pw else default_pw

    if "synapse" in sources:
        if "redshift_admin_password" not in creds and "postgres_admin_password" not in creds:
            console.print(f"  [dim]デフォルト password: {default_pw}[/dim]")
        pw = questionary.password(
            "Azure Synapse admin password (空白 Enter = 自動生成):",
        ).ask()
        creds["synapse_admin_password"] = pw if pw else default_pw
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
        default_project = creds.get("gcp_project_id", "")
        creds["gcp_project_id"] = questionary.text(
            "GCP project ID:",
            default=default_project,
            validate=lambda x: len(x) > 0 or "必須です",
        ).ask()
        creds["gcp_credentials_json"] = questionary.text(
            "GCP SA key JSON path (空白 = 自動作成):",
            default="",
        ).ask()
        if creds["gcp_credentials_json"]:
            key_path = Path(creds["gcp_credentials_json"]).expanduser()
            if key_path.exists():
                creds["gcp_credentials_json"] = key_path.read_text()
            else:
                console.print(f"[red]File not found: {key_path}[/red]")
                sys.exit(1)
        else:
            # Auto-create SA and key JSON
            creds["gcp_credentials_json"] = _create_gcp_sa_key(creds["gcp_project_id"])

    return creds


def generate_tfvars(
    cloud: str,
    workspace_url: str,
    sources: list[str],
    query_prefix: str,
    catalog_prefix: str,
    analysis_catalog: str,
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

    # Derive db_prefix from query_prefix (strip _query suffix)
    db_prefix = query_prefix.removesuffix("_query")

    lines += [
        "",
        "# Catalog naming",
        f'catalog_prefix_query   = "{query_prefix}"',
        f'catalog_prefix_catalog = "{catalog_prefix}"',
        f'analysis_catalog       = "{analysis_catalog}"',
        f'db_prefix              = "{db_prefix}"',
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


def generate_notebook(
    sources: list[str],
    query_prefix: str = "lhf_query",
    catalog_prefix: str = "lhf_catalog",
    analysis_catalog: str = "main",
):
    """Generate federation_demo.sql with only the selected sources' sections."""
    db_prefix = query_prefix.removesuffix("_query")
    template = NOTEBOOK_TEMPLATE.read_text()

    # Inject actual values into DECLARE defaults
    replacements = {
        "query_prefix STRING DEFAULT 'lhf_query'": f"query_prefix STRING DEFAULT '{query_prefix}'",
        "catalog_prefix STRING DEFAULT 'lhf_catalog'": f"catalog_prefix STRING DEFAULT '{catalog_prefix}'",
        "db_prefix STRING DEFAULT 'lhf_demo'": f"db_prefix STRING DEFAULT '{db_prefix}'",
        "analysis_catalog STRING DEFAULT 'main'": f"analysis_catalog STRING DEFAULT '{analysis_catalog}'",
    }
    for old, new in replacements.items():
        template = template.replace(old, new)

    commands = template.split("-- COMMAND ----------")

    output_commands = []
    current_source = None
    skip_source = False

    for cmd in commands:
        # Check if this command starts a new source section (1.1 - 1.6)
        new_source = None
        for src, patterns in SOURCE_SECTIONS.items():
            if any(p in cmd for p in patterns):
                new_source = src
                break

        if new_source:
            current_source = new_source
            skip_source = new_source not in sources
            if skip_source:
                continue
            output_commands.append(cmd)
        elif current_source and skip_source:
            # Check if we've left the source section (hit a new chapter)
            if any(m in cmd for m in ["# 第2章:", "# 第3章:", "# 第4章:"]):
                current_source = None
                skip_source = False
                output_commands.append(cmd)
        else:
            # Skip source-specific cross-source JOINs in chapter 2
            skip_ch2 = any(
                marker in cmd and src not in sources
                for src, marker in CH2_SOURCE_MARKERS.items()
            )
            if skip_ch2:
                continue

            # Skip machine_health_summary if glue+redshift not both enabled
            if "machine_health_summary" in cmd and ("CREATE OR REPLACE TABLE" in cmd or "ORDER BY sensor_critical_count" in cmd):
                if "glue" not in sources or "redshift" not in sources:
                    continue

            # Skip factory_operations_union if not all 5 sources enabled
            all_five = {"glue", "redshift", "postgres", "synapse", "bigquery"}
            if "factory_operations_union" in cmd:
                if not all_five.issubset(set(sources)):
                    continue

            # Remove cross-source JOIN header if no extra sources
            if "追加ソースのクロスソース" in cmd:
                if not any(s in sources for s in ["postgres", "synapse", "bigquery"]):
                    continue

            output_commands.append(cmd)

    job_content = "-- COMMAND ----------".join(output_commands)
    NOTEBOOK_OUTPUT.write_text(job_content)

    # Generate widget-based interactive version (cleaner SQL for UI use)
    widget_content = job_content
    # Replace DECLARE with CREATE WIDGET
    widget_content = widget_content.replace(
        "DECLARE OR REPLACE query_prefix STRING", "CREATE WIDGET TEXT query_prefix"
    ).replace(
        "DECLARE OR REPLACE catalog_prefix STRING", "CREATE WIDGET TEXT catalog_prefix"
    ).replace(
        "DECLARE OR REPLACE db_prefix STRING", "CREATE WIDGET TEXT db_prefix"
    ).replace(
        "DECLARE OR REPLACE analysis_catalog STRING", "CREATE WIDGET TEXT analysis_catalog"
    )
    # Replace IDENTIFIER() calls with ${} syntax
    # Pattern: IDENTIFIER(var || '_suffix.schema.table')
    def _replace_identifier(m: re.Match) -> str:
        expr = m.group(1).strip()
        # Split by || and reconstruct with ${} syntax
        parts = [p.strip().strip("'") for p in expr.split("||")]
        result = ""
        for p in parts:
            if p in ("query_prefix", "catalog_prefix", "db_prefix", "analysis_catalog"):
                result += f"${{{p}}}"
            else:
                result += p
        return result
    widget_content = re.sub(r"IDENTIFIER\(([^)]+)\)", _replace_identifier, widget_content)
    # Replace EXECUTE IMMEDIATE with direct SQL (for SHOW GRANTS)
    widget_content = widget_content.replace(
        "EXECUTE IMMEDIATE 'SHOW GRANTS ON CATALOG ' || query_prefix || '_redshift'",
        "SHOW GRANTS ON CATALOG ${query_prefix}_redshift",
    )
    # Replace EXECUTE IMMEDIATE for CREATE TABLE (machine_health_summary)
    widget_content = re.sub(
        r"EXECUTE IMMEDIATE\n'(CREATE OR REPLACE TABLE )'.*?';",
        lambda m: _convert_execute_immediate(m.group(0)),
        widget_content,
        flags=re.DOTALL,
    )
    # Update header
    widget_content = widget_content.replace(
        "デプロイ時に自動設定されます。手動で変更する場合は以下を編集してください。",
        "ノートブック上部の Widget で値を変更できます。デプロイ時に自動設定済みです。",
    )
    NOTEBOOK_WIDGET_OUTPUT.write_text(widget_content)
    console.print(f"[green]✓[/green] Generated notebook with {len(sources)} source(s): {', '.join(sources)}")
    console.print(f"[green]✓[/green] Generated interactive notebook (widget版)")


def _convert_execute_immediate(sql: str) -> str:
    """Convert EXECUTE IMMEDIATE dynamic SQL to plain SQL with ${} variables."""
    # Remove EXECUTE IMMEDIATE wrapper
    sql = sql.replace("EXECUTE IMMEDIATE\n'", "")
    # Remove trailing ';
    if sql.endswith("';"):
        sql = sql[:-2] + ";"
    # Replace variable concatenation: ' || var || '  →  ${var}
    sql = re.sub(
        r"' \|\| (\w+) \|\| '",
        lambda m: "${" + m.group(1) + "}",
        sql,
    )
    # Replace trailing: ' || var || '...  patterns at end of lines
    sql = re.sub(
        r"' \|\| (\w+) \|\| '",
        lambda m: "${" + m.group(1) + "}",
        sql,
    )
    # Replace escaped quotes: \' → '
    sql = sql.replace("\\'", "'")
    return sql


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

    for attempt in range(1, 4):
        result = subprocess.run(
            ["databricks", "bundle", "deploy", "--target", "dev"],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True, text=True,
        )

        if result.returncode != 0:
            console.print(f"  [yellow]Attempt {attempt}/3 failed: {result.stderr.strip()}[/yellow]")
            if attempt < 3:
                time.sleep(5)
            continue

        # Verify deployment by checking notebook exists
        token = _get_databricks_token(workspace_url)
        if token:
            try:
                nb_path = _get_notebook_path(workspace_url, token)
                check = subprocess.run(
                    ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                     "-G", f"{workspace_url}/api/2.0/workspace/get-status",
                     "-H", f"Authorization: Bearer {token}",
                     "--data-urlencode", f"path={nb_path}"],
                    capture_output=True, text=True, timeout=15,
                )
                if check.stdout.strip() == "200":
                    console.print("[green]✓ DAB deploy complete (notebook verified)[/green]\n")
                    return
                else:
                    console.print(f"  [yellow]Attempt {attempt}/3: deploy reported success but notebook not found[/yellow]")
                    if attempt < 3:
                        time.sleep(5)
                    continue
            except Exception:
                pass

        # If we can't verify, trust the deploy result
        console.print("[green]✓ DAB deploy complete[/green]\n")
        return

    console.print("[yellow]! DAB deploy failed after 3 attempts (non-critical)[/yellow]\n")


def run_notebook_job(workspace_url: str, query_prefix: str, catalog_prefix: str):
    """Create and run a one-time job to execute the demo notebook."""
    console.print("[bold]Running demo notebook...[/bold]\n")

    token = _get_databricks_token(workspace_url)
    if not token:
        console.print("[yellow]! Could not get token. Skipping notebook run.[/yellow]\n")
        return

    warehouse_id = _get_warehouse_id(workspace_url, token)
    if not warehouse_id:
        console.print("[yellow]! No SQL warehouse found. Skipping notebook run.[/yellow]\n")
        return

    # Derive db_prefix from terraform outputs
    outputs = get_terraform_outputs()
    db_names = outputs.get("database_names", {})
    sample_db = next(iter(db_names.values()), "lhf_demo_factory")
    db_prefix = sample_db.rsplit("_factory", 1)[0]

    notebook_path = _get_notebook_path(workspace_url, token)

    job_payload = {
        "run_name": "Federation Demo - Validation Run",
        "tasks": [{
            "task_key": "run_demo",
            "notebook_task": {
                "notebook_path": notebook_path,
                "source": "WORKSPACE",
            },
            "environment_key": "default",
        }],
        "environments": [{
            "environment_key": "default",
            "spec": {"client": "1"},
        }],
    }

    try:
        result = subprocess.run(
            ["curl", "-s", "-X", "POST",
             f"{workspace_url}/api/2.1/jobs/runs/submit",
             "-H", f"Authorization: Bearer {token}",
             "-H", "Content-Type: application/json",
             "-d", json.dumps(job_payload)],
            capture_output=True, text=True, timeout=30,
        )
        resp = json.loads(result.stdout)
        run_id = resp.get("run_id")

        if not run_id:
            console.print(f"[yellow]! Job submit failed: {resp}[/yellow]\n")
            return

        console.print(f"  Run ID: {run_id}")
        console.print(f"  URL: {workspace_url}/#job/run/{run_id}\n")

        # Poll for completion (max 10 min)
        for _ in range(60):
            time.sleep(10)
            poll = subprocess.run(
                ["curl", "-s",
                 f"{workspace_url}/api/2.1/jobs/runs/get?run_id={run_id}",
                 "-H", f"Authorization: Bearer {token}"],
                capture_output=True, text=True, timeout=15,
            )
            run_info = json.loads(poll.stdout)
            state = run_info.get("state", {})
            life_cycle = state.get("life_cycle_state", "")
            result_state = state.get("result_state", "")

            if life_cycle == "TERMINATED":
                if result_state == "SUCCESS":
                    console.print("[green]✓ Notebook run completed successfully[/green]\n")
                else:
                    msg = state.get("state_message", "")
                    console.print(f"[yellow]! Notebook run finished: {result_state} - {msg}[/yellow]\n")
                return
            elif life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
                console.print(f"[red]✗ Notebook run failed: {life_cycle}[/red]\n")
                return

        console.print("[yellow]! Notebook run timed out (10 min). Check manually.[/yellow]\n")

    except Exception as e:
        console.print(f"[yellow]! Notebook job error: {e}[/yellow]\n")


def _get_notebook_path(workspace_url: str, token: str, cloud: str = "") -> str:
    """Get the deployed notebook path based on current user and cloud."""
    try:
        result = subprocess.run(
            ["curl", "-s", f"{workspace_url}/api/2.0/preview/scim/v2/Me",
             "-H", f"Authorization: Bearer {token}"],
            capture_output=True, text=True, timeout=15,
        )
        user_name = json.loads(result.stdout).get("userName", "unknown")
    except Exception:
        user_name = "unknown"
    # Determine notebook suffix from cloud or tfvars
    if not cloud:
        cloud = _read_tfvar("cloud")
    suffix = cloud if cloud in ("aws", "azure") else "azure"
    return f"/Users/{user_name}/.bundle/lakehouse_federation_demo/files/notebooks/federation_demo_{suffix}"


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


def _read_tfvar(key: str) -> str:
    """Read a single value from terraform.tfvars."""
    tfvars_path = TERRAFORM_DIR / "terraform.tfvars"
    if not tfvars_path.exists():
        return ""
    for line in tfvars_path.read_text().splitlines():
        line = line.strip()
        if line.startswith(key) and "=" in line:
            val = line.split("=", 1)[1].strip().strip('"')
            return val
    return ""


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

    aws_region = _read_tfvar("aws_region") or "us-west-2"
    gcp_project_id = _read_tfvar("gcp_project_id")
    project_prefix = _read_tfvar("project_prefix") or "lhf-demo"

    # Derive db_prefix (custom schema name) from any database name
    sample_db = next(iter(db_names.values()), "lhf_demo_factory")
    db_prefix = sample_db.rsplit("_factory", 1)[0]

    # Get notebook path
    token = _get_databricks_token(workspace_url)
    nb_path = _get_notebook_path(workspace_url, token) if token else "/unknown"

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
        "### Databricks",
        "",
        f"| Resource | URL |",
        f"|----------|-----|",
        f"| Workspace | {workspace_url} |",
        f"| Demo Notebook (Job用) | {workspace_url}/#workspace{nb_path} |",
        f"| Demo Notebook (UI用 Widget版) | {workspace_url}/#workspace{nb_path.replace('federation_demo', 'federation_demo_interactive')} |",
        f"| Catalog Explorer | {workspace_url}/explore/data |",
    ]

    # Add catalog links for each source
    for src in sources:
        cat_name = catalogs.get(src)
        if cat_name:
            lines.append(f"| {SOURCES[src]['label']} Catalog | {workspace_url}/explore/data/{cat_name} |")

    lines += ["", "### External Source Consoles", ""]
    lines += ["| Source | Console / Query Editor |", "|--------|----------------------|"]

    if "glue" in sources:
        glue_db = db_names.get("glue", "")
        lines.append(f"| AWS Glue | https://{aws_region}.console.aws.amazon.com/glue/home?region={aws_region}#/v2/data-catalog/databases/view/{glue_db} |")
        lines.append(f"| S3 (Glue Data) | https://s3.console.aws.amazon.com/s3/buckets/{outputs.get('s3_bucket_name', '')}?region={aws_region} |")
    if "redshift" in sources:
        lines.append(f"| Redshift Query Editor | https://{aws_region}.console.aws.amazon.com/sqlworkbench/home?region={aws_region}#/client |")
    if "postgres" in sources and cloud == "aws":
        lines.append(f"| RDS (PostgreSQL) | https://{aws_region}.console.aws.amazon.com/rds/home?region={aws_region}#database:id={project_prefix}-postgres |")
    if "synapse" in sources:
        synapse_ep = outputs.get("synapse_endpoint", "")
        synapse_ws_name = synapse_ep.replace("-ondemand.sql.azuresynapse.net", "") if synapse_ep else ""
        lines.append(f"| Azure Synapse Studio | https://web.azuresynapse.net?workspace={synapse_ws_name} |")
    if "bigquery" in sources and gcp_project_id:
        bq_dataset = db_names.get("bigquery", "")
        lines.append(f"| BigQuery Console | https://console.cloud.google.com/bigquery?project={gcp_project_id}&d={bq_dataset}&p={gcp_project_id}&page=dataset |")

    lines += ["", "### Connection Endpoints (CLI / JDBC)", ""]
    lines += ["| Source | Endpoint |", "|--------|----------|"]

    if "redshift" in sources and outputs.get("redshift_endpoint"):
        lines.append(f"| Redshift | `{outputs['redshift_endpoint']}:5439` |")
    if "postgres" in sources and outputs.get("postgres_endpoint"):
        lines.append(f"| PostgreSQL | `{outputs['postgres_endpoint']}:5432` |")
    if "synapse" in sources and outputs.get("synapse_endpoint"):
        lines.append(f"| Synapse | `{outputs['synapse_endpoint']}:1433` |")

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
        schema = db_names.get(src, "unknown") if src == "bigquery" else db_prefix
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
        ("sensor_readings", "{query_prefix}_redshift.{db_prefix}.sensor_readings", 100),
        ("production_events", "{query_prefix}_redshift.{db_prefix}.production_events", 30),
        ("quality_inspections", "{query_prefix}_redshift.{db_prefix}.quality_inspections", 40),
    ],
    "postgres": [
        ("maintenance_logs", "{query_prefix}_postgres.{db_prefix}.maintenance_logs", 30),
        ("work_orders", "{query_prefix}_postgres.{db_prefix}.work_orders", 25),
    ],
    "synapse": [
        ("shift_schedules", "{query_prefix}_synapse.{db_prefix}.shift_schedules", 40),
        ("energy_consumption", "{query_prefix}_synapse.{db_prefix}.energy_consumption", 50),
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
    """Find a running SQL warehouse, starting one if needed."""
    try:
        result = subprocess.run(
            ["curl", "-s", f"{workspace_url}/api/2.0/sql/warehouses",
             "-H", f"Authorization: Bearer {token}"],
            capture_output=True, text=True, timeout=15,
        )
        warehouses = json.loads(result.stdout).get("warehouses", [])
        # Prefer a running warehouse
        for w in warehouses:
            if w.get("state") == "RUNNING":
                return w["id"]
        # If none running, try to start the first stopped one
        for w in warehouses:
            if w.get("state") == "STOPPED":
                wh_id = w["id"]
                console.print(f"  Starting stopped warehouse {w.get('name', wh_id)}...")
                subprocess.run(
                    ["curl", "-s", "-X", "POST",
                     f"{workspace_url}/api/2.0/sql/warehouses/{wh_id}/start",
                     "-H", f"Authorization: Bearer {token}"],
                    capture_output=True, text=True, timeout=15,
                )
                # Wait for warehouse to start
                for _ in range(24):  # up to ~2 minutes
                    time.sleep(5)
                    check = subprocess.run(
                        ["curl", "-s",
                         f"{workspace_url}/api/2.0/sql/warehouses/{wh_id}",
                         "-H", f"Authorization: Bearer {token}"],
                        capture_output=True, text=True, timeout=15,
                    )
                    state = json.loads(check.stdout).get("state", "")
                    if state == "RUNNING":
                        console.print(f"  [green]✓[/green] Warehouse started.")
                        return wh_id
                    if state in ("DELETED", "DELETING"):
                        break
                console.print(f"  [yellow]! Warehouse did not start in time.[/yellow]")
                return wh_id  # Return anyway, SQL API may queue
        # Fallback: return first warehouse regardless of state
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


def save_deploy_state(
    workspace_url: str,
    sources: list[str],
    query_prefix: str,
    catalog_prefix: str,
    analysis_catalog: str,
):
    """Save deploy state for later cleanup/destroy."""
    state = {
        "workspace_url": workspace_url,
        "sources": sources,
        "query_prefix": query_prefix,
        "catalog_prefix": catalog_prefix,
        "analysis_catalog": analysis_catalog,
        "deployed_at": datetime.now(timezone.utc).isoformat(),
    }
    # Save AWS_PROFILE if set (needed for destroy)
    aws_profile = os.environ.get("AWS_PROFILE")
    if aws_profile:
        state["aws_profile"] = aws_profile
    DEPLOY_STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")
    console.print(f"[green]✓[/green] Saved deploy state to {DEPLOY_STATE_FILE.name}")


def load_deploy_state() -> dict | None:
    """Load deploy state from file."""
    if not DEPLOY_STATE_FILE.exists():
        return None
    try:
        return json.loads(DEPLOY_STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def cleanup_notebook_objects(workspace_url: str, analysis_catalog: str):
    """Drop table and schema created by the demo notebook."""
    console.print("\n[bold]Cleaning up notebook-created objects...[/bold]")

    token = _get_databricks_token(workspace_url)
    if not token:
        console.print("[yellow]! Could not get token. Skipping cleanup.[/yellow]")
        return

    warehouse_id = _get_warehouse_id(workspace_url, token)
    if not warehouse_id:
        console.print("[yellow]! No SQL warehouse found. Skipping cleanup.[/yellow]")
        return

    cleanup_sqls = [
        (f"DROP TABLE IF EXISTS {analysis_catalog}.lhf_demo.factory_operations_union", "factory_operations_union"),
        (f"DROP TABLE IF EXISTS {analysis_catalog}.lhf_demo.machine_health_summary", "machine_health_summary"),
        (f"DROP SCHEMA IF EXISTS {analysis_catalog}.lhf_demo CASCADE", "lhf_demo schema"),
    ]

    for sql, label in cleanup_sqls:
        try:
            resp = _execute_sql(workspace_url, token, warehouse_id, sql)
            status = resp.get("status", {}).get("state", "")
            if status == "SUCCEEDED":
                console.print(f"  [green]✓[/green] Dropped {label}")
            else:
                err = resp.get("status", {}).get("error", {}).get("message", "unknown")
                console.print(f"  [yellow]![/yellow] {label}: {err}")
        except Exception as e:
            console.print(f"  [yellow]![/yellow] {label}: {e}")


def _is_interactive() -> bool:
    """Check if stdin is a terminal (interactive mode)."""
    return sys.stdin.isatty()


def _ensure_aws_auth(sources: list[str]):
    """Ensure AWS auth is valid, attempting SSO login if needed."""
    needs_aws = any(s in sources for s in ["glue", "redshift", "postgres"])
    if not needs_aws:
        return

    if _aws_auth_ok():
        profile = os.environ.get("AWS_PROFILE", "default")
        console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
        return

    # Try to find and login with an SSO profile
    profile = os.environ.get("AWS_PROFILE", "")
    if not profile:
        # Auto-discover a sandbox-field-eng profile (prefer sandbox-admin)
        try:
            profiles_result = subprocess.run(
                ["aws", "configure", "list-profiles"],
                capture_output=True, text=True, timeout=5,
            )
            all_profiles = [p.strip() for p in profiles_result.stdout.splitlines()]
            for p in all_profiles:
                if "sandbox-field-eng" in p and "sandbox-admin" in p:
                    profile = p
                    break
            if not profile:
                for p in all_profiles:
                    if "sandbox-field-eng" in p and ("admin" in p or "power-user" in p):
                        profile = p
                        break
        except Exception:
            pass

    if profile:
        os.environ["AWS_PROFILE"] = profile
        if _aws_auth_ok():
            console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
            return
        console.print(f"  SSO ログイン実行中... (profile: {profile})")
        subprocess.run(["aws", "sso", "login", "--profile", profile], timeout=120)
        if _aws_auth_ok():
            console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
            return

    console.print("[red]✗ AWS 認証失敗。`aws sso login` を実行してから再試行してください。[/red]")
    sys.exit(1)


def _ensure_azure_auth(sources: list[str]):
    """Ensure Azure auth is valid."""
    needs_azure = any(s in sources for s in ["synapse", "onelake"])
    if not needs_azure:
        return
    try:
        result = subprocess.run(
            ["az", "account", "show", "--query", "id", "-o", "tsv"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            console.print(f"  [green]✓[/green] Azure 認証済み (subscription: {result.stdout.strip()})")
        else:
            console.print("[red]✗ Azure 未認証。`az login` を実行してから再試行してください。[/red]")
            sys.exit(1)
    except FileNotFoundError:
        console.print("[yellow]! az CLI が見つかりません[/yellow]")


def destroy():
    """Destroy all deployed resources: notebook objects → terraform → DAB."""
    console.print(Panel.fit(
        "[bold red]Lakehouse Federation Demo - Destroy[/bold red]\n"
        "[dim]Removing all deployed resources[/dim]",
        border_style="red",
    ))

    state = load_deploy_state()
    if not state:
        console.print("[yellow]No deploy state found (.deploy_state.json).[/yellow]")
        console.print("[dim]Attempting destroy with terraform state only...[/dim]\n")
        subprocess.run(["terraform", "destroy", "-auto-approve"], cwd=TERRAFORM_DIR)
        return

    workspace_url = state["workspace_url"]
    analysis_catalog = state.get("analysis_catalog", "main")
    sources = state.get("sources", [])

    # Restore AWS_PROFILE from deploy state
    aws_profile = state.get("aws_profile")
    if aws_profile and not os.environ.get("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = aws_profile
        console.print(f"  AWS Profile: {aws_profile}")

    console.print(f"  Workspace: {workspace_url}")
    console.print(f"  Analysis catalog: {analysis_catalog}")
    console.print(f"  Sources: {', '.join(sources)}")
    console.print(f"  Deployed at: {state.get('deployed_at', 'unknown')}\n")

    # Check cloud auth non-interactively
    _ensure_aws_auth(sources)
    _ensure_azure_auth(sources)

    if _is_interactive():
        if not questionary.confirm("全リソースを削除しますか?", default=False).ask():
            console.print("[yellow]Cancelled.[/yellow]")
            return
    else:
        console.print("[dim]Non-interactive mode: proceeding with destroy[/dim]")

    # 1. Setup Databricks auth for SQL cleanup
    setup_databricks_auth(workspace_url)

    # 2. Cleanup notebook-created objects via SQL
    cleanup_notebook_objects(workspace_url, analysis_catalog)

    # 3. Terraform destroy
    console.print("\n[bold]Terraform destroy...[/bold]\n")
    result = subprocess.run(["terraform", "destroy", "-auto-approve"], cwd=TERRAFORM_DIR)
    if result.returncode == 0:
        console.print("[green]✓ Terraform destroy complete[/green]\n")
    else:
        console.print("[red]✗ Terraform destroy failed[/red]\n")

    # 4. DAB destroy
    console.print("[bold]DAB destroy...[/bold]\n")
    env = os.environ.copy()
    env["DATABRICKS_HOST"] = workspace_url
    result = subprocess.run(
        ["databricks", "bundle", "destroy", "--target", "dev", "--auto-approve"],
        cwd=PROJECT_ROOT, env=env,
    )
    if result.returncode == 0:
        console.print("[green]✓ DAB destroy complete[/green]\n")
    else:
        console.print("[yellow]! DAB destroy failed (non-critical)[/yellow]\n")

    # 4. Remove state file
    if DEPLOY_STATE_FILE.exists():
        DEPLOY_STATE_FILE.unlink()
        console.print("[green]✓[/green] Removed deploy state file\n")

    console.print("[bold green]Destroy complete.[/bold green]")


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

    token = _get_databricks_token(workspace_url)
    nb_path = _get_notebook_path(workspace_url, token) if token else "/unknown"
    console.print(f"\n[bold]Databricks Workspace:[/bold] {workspace_url}")
    console.print(f"[bold]Demo Notebook:[/bold] {workspace_url}/#workspace{nb_path}")

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
    query_prefix, catalog_prefix, analysis_catalog = get_catalog_prefix()
    auto_creds = check_cloud_auth(cloud, sources)
    setup_databricks_auth(workspace_url)
    creds = collect_credentials(cloud, sources, auto_creds)

    # Confirm
    console.print("\n[bold]Configuration Summary:[/bold]")
    console.print(f"  Cloud: {cloud}")
    console.print(f"  Workspace: {workspace_url}")
    console.print(f"  Sources: {', '.join(sources)}")
    console.print(f"  Prefixes: {query_prefix} / {catalog_prefix}")
    console.print(f"  Analysis catalog: {analysis_catalog}")

    if not questionary.confirm("\nデプロイを開始しますか?", default=True).ask():
        console.print("[yellow]Cancelled.[/yellow]")
        sys.exit(0)

    generate_tfvars(cloud, workspace_url, sources, query_prefix, catalog_prefix, analysis_catalog, creds)
    generate_notebook(sources, query_prefix, catalog_prefix, analysis_catalog=analysis_catalog)
    save_deploy_state(workspace_url, sources, query_prefix, catalog_prefix, analysis_catalog)
    run_terraform()
    deploy_dab(workspace_url)
    run_notebook_job(workspace_url, query_prefix, catalog_prefix)
    print_summary(cloud, workspace_url, sources, query_prefix, catalog_prefix)


def redeploy():
    """Non-interactive redeploy using existing terraform.tfvars and deploy state."""
    print_banner()

    # Read config from existing terraform.tfvars
    tfvars_path = TERRAFORM_DIR / "terraform.tfvars"
    if not tfvars_path.exists():
        console.print("[red]No terraform.tfvars found. Run interactive deploy first.[/red]")
        sys.exit(1)

    cloud = _read_tfvar("cloud")
    workspace_url = _read_tfvar("databricks_host")
    query_prefix = _read_tfvar("catalog_prefix_query") or "lhf_query"
    catalog_prefix = _read_tfvar("catalog_prefix_catalog") or "lhf_catalog"

    # Read sources from enable_* flags
    sources = []
    for key in SOURCES:
        val = _read_tfvar(f"enable_{key}")
        if val == "true":
            sources.append(key)

    # Read analysis_catalog from deploy state or terraform.tfvars
    state = load_deploy_state()
    analysis_catalog = (state or {}).get(
        "analysis_catalog",
        _read_tfvar("analysis_catalog") or "main",
    )

    console.print(f"[bold]Redeploy (non-interactive)[/bold]")
    console.print(f"  Cloud: {cloud}")
    console.print(f"  Workspace: {workspace_url}")
    console.print(f"  Sources: {', '.join(sources)}")
    console.print(f"  Prefixes: {query_prefix} / {catalog_prefix}")
    console.print(f"  Analysis catalog: {analysis_catalog}\n")

    # Restore AWS_PROFILE from deploy state
    aws_profile = (state or {}).get("aws_profile")
    if aws_profile and not os.environ.get("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = aws_profile
        console.print(f"  AWS Profile: {aws_profile}")

    # Check auth non-interactively
    _ensure_aws_auth(sources)
    _ensure_azure_auth(sources)
    setup_databricks_auth(workspace_url)

    generate_notebook(sources, query_prefix, catalog_prefix, analysis_catalog=analysis_catalog)
    save_deploy_state(workspace_url, sources, query_prefix, catalog_prefix, analysis_catalog)
    run_terraform()
    deploy_dab(workspace_url)
    run_connectivity_test(workspace_url, sources, query_prefix, catalog_prefix)
    run_notebook_job(workspace_url, query_prefix, catalog_prefix)
    generate_deploy_result(cloud, workspace_url, sources, query_prefix, catalog_prefix)
    console.print("\n[bold green]Redeploy complete.[/bold green]")


if __name__ == "__main__":
    if "--destroy" in sys.argv:
        destroy()
    elif "--redeploy" in sys.argv:
        redeploy()
    else:
        main()
