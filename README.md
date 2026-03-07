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

### AWS 全般

#### "Cannot assume role" エラー
- IAM ロールの trust policy が正しいか AWS Console で確認
- `terraform apply` を再実行

#### AWS SSO トークン期限切れ
- `aws sso login --profile <profile-name>` で再認証
- `AWS_PROFILE` 環境変数がセットされていることを確認

### Glue / Lake Formation

#### "External location validation failed"
- Storage Credential の IAM ロールが S3 への read アクセス権を持っているか確認

#### "Insufficient Lake Formation permission(s)"
- Lake Formation のデフォルト権限は**新規作成**テーブルにのみ適用される
- 既存テーブルには `aws lakeformation grant-permissions` で個別に権限付与が必要
- SSO ロールを Lake Formation admin に設定: `aws lakeformation put-data-lake-settings`
- 詳細: `IAM_ALLOWED_PRINCIPALS` に ALL 権限を付与する

#### Glue テーブルが Databricks から見えない
1. Lake Formation admin が正しく設定されているか確認
2. 各テーブルに `IAM_ALLOWED_PRINCIPALS` 権限があるか確認:
   ```bash
   aws lakeformation list-permissions --resource-type TABLE
   ```

### Redshift

#### "Connection test failed"
- Redshift Serverless が AVAILABLE 状態か確認
- Security Group で 5439 ポートが開いているか確認

### PostgreSQL

#### 接続エラー
- Security Group で 5432 ポートが開いているか確認
- `postgres_admin_password` が正しいか確認
- macOS で `psql` が見つからない場合: `/opt/homebrew/opt/libpq/bin/psql` を使用

#### Terraform init でテーブルが作成されない
- `psql` が Terraform の `local-exec` 実行環境の PATH に存在しない場合、スクリプトが静かに失敗する
- Terraform は init スクリプトの完了を報告するが、実際にはテーブルが空のまま
- 対策: `set -e` と `command -v psql` による PATH 検出を init スクリプトに追加済み
- 手動確認: `psql -h <endpoint> -U pgadmin -d <db_name> -c '\dt public.*'`

### Azure Synapse

#### ファイアウォールルール作成失敗
- Azure Policy が `0.0.0.0-255.255.255.255` を拒否する場合がある
- Terraform は自動的に2つの広範囲ルール（1.0.0.0-126.x, 128.0.0.0-254.x）で回避

#### Serverless SQL pool の制限事項
- **INSERT 不可**: テーブル作成+INSERT ではなく、VIEW with VALUES を使用
- **DATABASE 作成**: SQL認証ではなく AAD トークンが必要（`az account get-access-token --resource https://sql.azuresynapse.net`）
- **master DB**: ユーザーオブジェクトの SELECT に制限あり → 専用DB を作成して使用
- **エンドポイント**: Serverless は `-ondemand.sql.azuresynapse.net` を使用
- **sp_addextendedproperty**: Serverless SQL pool では非対応。テーブル/カラムコメントは設定不可
- **ファイアウォール反映遅延**: ルール作成後、接続可能になるまで10-15秒の遅延がある

#### Terraform init でデータベースが作成されない
- ファイアウォールルール適用直後の接続はタイムアウトすることがある
- `sqlcmd` の AAD トークン認証が初回で失敗する場合がある
- 対策: init スクリプトにリトライループと `sleep 15` を追加済み
- 手動確認: `sqlcmd -S <endpoint> -d master -P "$TOKEN" -G -Q "SELECT name FROM sys.databases"`

#### Databricks から "InvocationTargetException"
- ファイアウォールルールが Databricks の IP を許可しているか確認
- `trustServerCertificate = "true"` がコネクション設定に含まれているか確認
- 接続先が ondemand エンドポイントであることを確認

#### Databricks SQLDW コネクションで `database` オプションエラー
- `database` オプションはコネクションではなく**カタログ**側で設定する

### BigQuery

#### 接続エラー
- GCP 認証が有効か確認: `gcloud auth application-default login`
- `gcp_project_id` が正しいか確認

#### SA の権限不足で terraform destroy が失敗する
- サービスアカウントに `roles/bigquery.dataEditor` が必要（テーブル作成・削除に必須）
- `roles/bigquery.dataViewer` + `roles/bigquery.jobUser` だけでは destroy 時に 403 エラー
- 権限追加: `gcloud projects add-iam-policy-binding <project> --member="serviceAccount:<sa>" --role="roles/bigquery.dataEditor" --condition=None`
- destroy が途中で失敗した場合: `bq rm -f --table <project>:<dataset>.<table>` で手動削除し、`terraform state rm` でステートから除外

### Databricks

#### OAuth トークン期限切れ
- `databricks auth login --host <workspace-url>` で再認証
- MCP ツール使用時はキャッシュされたトークンが古い場合がある → CLI で直接 API 呼び出し

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
    └── sql/                                       # DDL/DML (ソース別)
        ├── redshift/                              # Redshift SQL
        ├── postgres/                              # PostgreSQL SQL
        ├── synapse/                               # Synapse SQL
        └── bigquery/                              # BigQuery SQL
```
