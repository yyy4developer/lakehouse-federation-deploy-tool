# Databricks Lakehouse Federation Demo

マルチクラウド対応の Lakehouse Federation デモ環境です。
AWS Glue、Amazon Redshift、PostgreSQL、Azure Synapse、Google BigQuery、Microsoft OneLake のデータに対して、
Databricks から Lakehouse Federation でクエリを実行します。

## アーキテクチャ

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Databricks Unity Catalog                        │
│                                                                      │
│  Catalog Federation            Query Federation                      │
│  ┌────────────────┐   ┌──────────────┐ ┌──────────────┐             │
│  │lhf_catalog_glue│   │lhf_query_    │ │lhf_query_    │             │
│  │   (AWS Glue)   │   │  redshift    │ │  postgres    │             │
│  └───────┬────────┘   └──────┬───────┘ └──────┬───────┘             │
│  ┌────────────────┐   ┌──────────────┐ ┌──────────────┐             │
│  │lhf_catalog_    │   │lhf_query_    │ │lhf_query_    │             │
│  │   onelake      │   │  synapse     │ │  bigquery    │             │
│  └───────┬────────┘   └──────┬───────┘ └──────┬───────┘             │
└──────────┼───────────────────┼─────────────────┼─────────────────────┘
           │                   │                 │
  ┌────────▼────────┐ ┌───────▼───────┐ ┌───────▼───────┐
  │  AWS Glue /     │ │ Redshift /    │ │ BigQuery /    │
  │  OneLake        │ │ PostgreSQL /  │ │ etc.          │
  │  (S3/ADLS直接)  │ │ Synapse (JDBC)│ │               │
  └─────────────────┘ └───────────────┘ └───────────────┘
```

## Federation ソース対応表

| ソース | Type | AWS Workspace | Azure Workspace |
|--------|------|:---:|:---:|
| AWS Glue | Catalog | O | - |
| OneLake (Fabric) | Catalog | - | O |
| Amazon Redshift | Query | O | O |
| PostgreSQL | Query | O (RDS) | O (Azure Flexible) |
| Azure Synapse | Query | O | O |
| Google BigQuery | Query | O | O |

## データテーマ: 工場生産管理

全テーブルが `machine_id` (1-10) を共通キーとしてJOIN可能です。

| ソース | テーブル | 行数 | 内容 |
|--------|---------|------|------|
| Glue | sensors, machines, quality_inspections | 20/10/50 | マスタ + 品質検査 |
| Redshift | sensor_readings, production_events, quality_inspections | 100/30/40 | トランザクション |
| PostgreSQL | maintenance_logs, work_orders | 30/25 | 保守・作業管理 |
| Synapse | shift_schedules, energy_consumption | 40/50 | シフト・電力 |
| BigQuery | downtime_records, cost_allocation | 35/30 | 停止・コスト |
| OneLake | production_plans, inventory_levels | 20/30 | 計画・在庫 |

## クイックスタート

### 1. ワンクリックデプロイ

```bash
./lakehouse_federation_demo_resource_deploy.sh
```

対話形式で以下を設定し、自動的に Terraform + DAB でデプロイします:
- クラウド選択 (AWS / Azure)
- Workspace URL (FEVM で作成可能)
- Federation ソース選択
- 認証情報の入力

### 2. 前提条件

| ツール | 必須 | 確認コマンド |
|--------|:----:|-------------|
| Terraform | Yes | `terraform version` |
| uv | Yes | `uv --version` |
| jq | Yes | `jq --version` |
| AWS CLI | AWS ソース使用時 | `aws --version` |
| Azure CLI | Azure ソース使用時 | `az --version` |
| gcloud CLI | BigQuery 使用時 | `gcloud --version` |
| Databricks CLI | DAB デプロイ時 | `databricks --version` |
| psql | PostgreSQL 使用時 | `psql --version` |

### 3. 認証

| Provider | 方法 | 確認コマンド |
|----------|------|------------|
| AWS | `aws configure` or env vars | `aws sts get-caller-identity` |
| Azure | `az login` | `az account show` |
| GCP | `gcloud auth application-default login` | `gcloud auth list` |
| Databricks | OAuth U2M (`databricks auth login --host <url>`) | `databricks auth token --host <url>` |

### 4. デモ実行

デプロイ完了後:
1. Databricks ワークスペースにログイン
2. **Workspace** → `/Shared/lakehouse_federation_demo/notebooks/federation_demo` を開く
3. SQL Warehouse (Pro/Serverless) をアタッチ
4. セル単位で実行（デプロイしたソースのセクションのみ）

## 手動デプロイ

対話スクリプトを使わず手動でデプロイする場合:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# terraform.tfvars を編集
terraform init
terraform plan
terraform apply
```

## クリーンアップ

```bash
cd terraform
terraform destroy
```

## トラブルシューティング

### "Cannot assume role" エラー
- IAM ロールの trust policy が正しいか AWS Console で確認
- `terraform apply` を再実行

### "Connection test failed" (Redshift)
- Redshift Serverless が AVAILABLE 状態か確認
- Security Group で 5439 ポートが開いているか確認

### "External location validation failed"
- Storage Credential の IAM ロールが S3 への read アクセス権を持っているか確認

### "Insufficient Lake Formation permission(s)"
- Lake Formation 権限は Terraform で自動管理されます。`terraform apply` を再実行

### PostgreSQL 接続エラー
- Security Group で 5432 ポートが開いているか確認
- `postgres_admin_password` が正しいか確認

### Synapse 接続エラー
- Synapse ファイアウォールルールが設定されているか確認
- `synapse_admin_password` が正しいか確認

### BigQuery 接続エラー
- GCP 認証が有効か確認: `gcloud auth application-default login`
- `gcp_project_id` が正しいか確認

## ファイル構成

```
lakehouse_federation/
├── lakehouse_federation_demo_resource_deploy.sh  # ワンクリックデプロイ
├── databricks.yml                                 # DAB 設定
├── pyproject.toml                                 # Python 依存 (uv)
├── scripts/
│   ├── deploy.py                                  # 対話式デプロイスクリプト
│   └── prerequisites.sh                           # CLI チェック
├── notebooks/
│   └── federation_demo.sql                        # デモノートブック (4章構成)
└── terraform/
    ├── main.tf                                    # Providers
    ├── variables.tf                               # 変数定義
    ├── outputs.tf                                 # 出力値
    ├── terraform.tfvars.example                   # 設定サンプル
    ├── aws_s3.tf, aws_iam.tf, aws_networking.tf   # AWS 基盤
    ├── aws_glue.tf, aws_glue_etl.tf               # Glue (Catalog Fed.)
    ├── aws_lakeformation.tf                       # Lake Formation
    ├── aws_redshift.tf, aws_redshift_data.tf      # Redshift (Query Fed.)
    ├── aws_rds_postgres.tf                        # PostgreSQL on AWS
    ├── azure_resource_group.tf                    # Azure 共通
    ├── azure_synapse.tf                           # Synapse (Query Fed.)
    ├── azure_postgres.tf                          # PostgreSQL on Azure
    ├── azure_onelake.tf                           # OneLake (Catalog Fed.)
    ├── gcp_bigquery.tf                            # BigQuery (Query Fed.)
    ├── databricks_credentials.tf                  # Credentials
    ├── databricks_connection.tf                   # Connections
    ├── databricks_catalog.tf                      # Foreign Catalogs
    ├── databricks_external.tf                     # External Location
    ├── scripts/generate_data.py                   # Glue ETL
    └── sql/                                       # DDL/DML
        ├── *.sql                                  # Redshift SQL
        ├── postgres/                              # PostgreSQL SQL
        ├── synapse/                               # Synapse SQL
        └── bigquery/                              # BigQuery SQL
```
