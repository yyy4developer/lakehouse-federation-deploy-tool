#!/usr/bin/env python3
"""
Lakehouse Federation Demo - Interactive Deploy Script
"""

import os
import subprocess
import sys
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

    # Check if already authenticated
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
        # Skip sources that require a different cloud
        if info["cloud_req"] and info["cloud_req"] != cloud:
            continue
        choices.append(questionary.Choice(info["label"], value=key))

    selected = questionary.checkbox(
        "有効にする Federation ソースを選択:",
        choices=choices,
        validate=lambda x: len(x) > 0 or "少なくとも1つ選択してください",
    ).ask()

    return selected


def get_catalog_prefix() -> tuple[str, str]:
    query_prefix = questionary.text(
        "Query Federation カタログ prefix:",
        default="lhf_query",
    ).ask()

    catalog_prefix = questionary.text(
        "Catalog Federation カタログ prefix:",
        default="lhf_catalog",
    ).ask()

    return query_prefix, catalog_prefix


def check_cloud_auth(cloud: str, sources: list[str]):
    """Check and guide cloud authentication."""
    console.print("\n[bold]認証チェック...[/bold]")

    # AWS check
    if cloud == "aws" or any(s in sources for s in ["glue", "redshift", "postgres"]):
        try:
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                console.print("  [green]✓[/green] AWS 認証済み")
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
                ["az", "account", "show"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                console.print("  [green]✓[/green] Azure 認証済み")
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


def collect_credentials(cloud: str, sources: list[str]) -> dict:
    """Collect passwords and credentials per source."""
    creds = {}

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
            # Multi-line JSON needs special handling
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


def print_summary(cloud: str, workspace_url: str, sources: list[str]):
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
    console.print(f"[bold]Demo Notebook:[/bold] {workspace_url}/#workspace/Shared/lakehouse_federation_demo/federation_demo")

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
    check_cloud_auth(cloud, sources)
    setup_databricks_auth(workspace_url)
    creds = collect_credentials(cloud, sources)

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
    print_summary(cloud, workspace_url, sources)


if __name__ == "__main__":
    main()
