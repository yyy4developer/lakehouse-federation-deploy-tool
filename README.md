# Databricks Lakehouse Federation Demo

AWS Glue と Amazon Redshift のデータに対して、Databricks から Lakehouse Federation でクエリを実行するデモ環境です。

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Workspace                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ glue_factory     │  │ redshift_factory  │  │ Demo Notebook │  │
│  │ (Foreign Catalog)│  │ (Foreign Catalog) │  │ (SQL queries) │  │
│  └────────┬────────┘  └────────┬─────────┘  └───────────────┘  │
│           │ Glue Connection     │ Redshift Connection           │
└───────────┼─────────────────────┼───────────────────────────────┘
            │                     │
            ▼                     ▼
┌───────────────────┐   ┌────────────────────────┐
│   AWS Glue        │   │  Redshift Serverless   │
│ ┌───────────────┐ │   │ ┌────────────────────┐ │
│ │ sensors (20)  │ │   │ │ sensor_readings    │ │
│ │ machines (10) │ │   │ │ (100 rows)         │ │
│ └───────┬───────┘ │   │ │ production_events  │ │
│         │ S3      │   │ │ (30 rows)          │ │
│         ▼         │   │ └────────────────────┘ │
│  ┌────────────┐   │   └────────────────────────┘
│  │  S3 Bucket │   │
│  │  (CSV)     │   │
│  └────────────┘   │
└───────────────────┘
```

**データテーマ: 工場生産センサーデータ**

- **Glue (マスターデータ)**: sensors（センサー情報）, machines（機械情報）
- **Redshift (トランザクションデータ)**: sensor_readings（センサー読取値）, production_events（生産イベント）
- **Cross-source JOIN キー**: `sensor_id`, `machine_id`

## 前提条件

以下がインストール・設定済みであること:

| ツール | バージョン | 確認コマンド |
|--------|-----------|-------------|
| [Terraform](https://developer.hashicorp.com/terraform/install) | >= 1.5.0 | `terraform version` |
| [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) | v2 | `aws --version` |
| Databricks Workspace | Unity Catalog 有効 | - |

## セットアップ手順

### Step 1: AWS 認証

AWS CLI でログインします。

```bash
# 方法A: アクセスキーで認証
aws configure
# AWS Access Key ID: ********
# AWS Secret Access Key: ********
# Default region name: us-west-2

# 方法B: SSO で認証
aws sso login --profile <your-profile>

# 認証確認
aws sts get-caller-identity
```

### Step 2: Databricks Personal Access Token (PAT) の発行

1. Databricks ワークスペース (https://e2-demo-field-eng.cloud.databricks.com) にログイン
2. 右上のユーザーアイコン → **Settings** をクリック
3. 左メニューの **Developer** → **Access tokens** をクリック
4. **Manage** → **Generate new token** をクリック
5. Comment: `lakehouse-federation-demo`、Lifetime: 任意（例: 90 days）
6. **Generate** → 表示されたトークン (`dapi...`) をコピー

> **重要**: トークンは一度しか表示されません。安全な場所に保存してください。

### Step 3: terraform.tfvars の設定

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

`terraform.tfvars` を編集して実際の値を入力:

```hcl
# Databricks
databricks_host  = "https://e2-demo-field-eng.cloud.databricks.com"
databricks_token = "dapi..."       # Step 2 で発行したトークン

# Redshift
redshift_admin_password = "MySecurePass123!"
```

> **NOTE**: AWS Account ID は `aws sts get-caller-identity` から自動取得されます。
> IAM ロールの External ID も Databricks が自動生成するため、手動入力は不要です。

### Step 4: デプロイ

```bash
cd terraform

# 初期化
terraform init

# プラン確認
terraform plan

# デプロイ実行
terraform apply
```

> **所要時間**: 約 5-10 分（Redshift Serverless の起動に時間がかかります）

デプロイ完了後、以下が出力されます:

```
Outputs:
  databricks_glue_catalog     = "glue_factory"
  databricks_redshift_catalog = "redshift_factory"
  redshift_endpoint           = "lhf-demo-wg.XXXXX.us-west-2.redshift-serverless.amazonaws.com"
  ...
```

### Step 5: デモ実行

1. Databricks ワークスペースにログイン
2. 左メニュー → **Workspace** をクリック
3. `notebooks/federation_demo.sql` の内容をコピーして新しいノートブックを作成
   - または: **Import** から `.sql` ファイルをアップロード
4. クラスター（DBR 13.3 LTS 以上）または SQL Warehouse（Pro/Serverless）をアタッチ
5. **Run All** でデモクエリを実行

## デモクエリのハイライト

### Single-source クエリ
- Glue: `SELECT * FROM glue_factory.lhf_demo_factory_master.sensors`
- Redshift: `SELECT * FROM redshift_factory.public.sensor_readings`

### Cross-source JOIN (Glue + Redshift)
```sql
-- センサーマスタ (Glue) + センサー読取値 (Redshift)
-- ※ Glue の OpenCSVSerde は全カラムを STRING で返すため try_cast を使用
SELECT s.sensor_name, s.sensor_type, r.value, r.status
FROM glue_factory.lhf_demo_factory_master.sensors s
JOIN redshift_factory.public.sensor_readings r
  ON try_cast(s.sensor_id AS INT) = r.sensor_id
WHERE r.status IN ('warning', 'critical');
```

### 4テーブル結合 (全データソース横断)
```sql
-- machines (Glue) + sensor_readings (Redshift) + sensors (Glue) + production_events (Redshift)
SELECT m.machine_name, s.sensor_name, r.value, r.status
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.sensor_readings r ON try_cast(m.machine_id AS INT) = r.machine_id
JOIN glue_factory.lhf_demo_factory_master.sensors s ON r.sensor_id = try_cast(s.sensor_id AS INT)
WHERE r.status != 'normal';
```

## クリーンアップ

```bash
cd terraform
terraform destroy
```

> 全ての AWS リソースと Databricks の Connection/Catalog が削除されます。

## トラブルシューティング

### "Cannot assume role" エラー
- IAM ロールの trust policy が正しく設定されているか AWS Console で確認
- `terraform apply` を再実行して IAM ロールの trust policy が最新の external_id を使っているか確認

### "Connection test failed" / "Failed to connect" (Redshift)
- Redshift Serverless のワークグループが AVAILABLE 状態か確認
- Security Group で 5439 ポートが開いているか確認
- `redshift_admin_password` が正しいか確認
- **Custodian に注意**: 共有 AWS アカウントでは `0.0.0.0/0` の ingress ルールが自動削除される場合があります。
  その場合は `terraform apply` で再適用してください（本デモでは `/1` CIDR に分割して回避しています）

### "External location validation failed"
- Storage Credential の IAM ロールが S3 バケットへの read アクセス権を持っているか確認
- S3 バケットに CSV ファイルがアップロードされているか確認

### "Insufficient Lake Formation permission(s)" (Glue)
- AWS Lake Formation が有効なアカウントでは、IAM ポリシーに加えて Lake Formation 権限が必要です
- `CreateDatabaseDefaultPermissions` が空の場合、作成した Glue データベースに `IAM_ALLOWED_PRINCIPALS` を手動で付与する必要があります
  ```bash
  aws lakeformation grant-permissions \
    --principal '{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"}' \
    --resource '{"Database":{"Name":"lhf_demo_factory_master"}}' \
    --permissions "ALL"
  ```
- Terraform の `aws_lakeformation_permissions` リソースで自動管理されます

### "Table not found" (Glue)
- Glue Database とテーブルが AWS Console で確認できるか確認
- Glue テーブルの S3 ロケーションが正しいか確認
- Foreign Catalog の `authorized_paths` が S3 パスをカバーしているか確認

### CAST エラー (Glue テーブル)
- Glue の `OpenCSVSerde` は全カラムを STRING として返します
- ヘッダー行もデータとして読まれるため、`CAST` ではなく `try_cast` を使用してください
- `try_cast` は変換できない値を NULL にし、JOIN で自動的に除外されます

### Redshift テーブルにデータがない
- `aws_redshiftdata_statement` が正常完了したか確認:
  ```bash
  aws redshift-data list-statements --region us-west-2
  ```
- ステートメントが FAILED の場合はエラー詳細を確認:
  ```bash
  aws redshift-data describe-statement --id <statement-id> --region us-west-2
  ```

### `databricks_credential` リソースでエラー
- Databricks Terraform provider のバージョンが 1.58 以上であることを確認
- `databricks_credential` が未サポートの場合は provider を最新版にアップデート:
  `terraform init -upgrade`
- それでもエラーが出る場合は、Databricks UI (Catalog > External data > Credentials)
  から手動で Service Credential を作成し、Terraform state から除外してください

## ファイル構成

```
lakehouse_federation/
├── terraform/
│   ├── main.tf                    # Providers (AWS, Databricks)
│   ├── variables.tf               # 変数定義
│   ├── terraform.tfvars.example   # 変数値サンプル
│   ├── outputs.tf                 # 出力値
│   ├── aws_networking.tf          # VPC, Subnets, Security Groups
│   ├── aws_s3.tf                  # S3バケット (Glueデータ格納)
│   ├── aws_iam.tf                 # IAMロール (Glue API + S3読取)
│   ├── aws_glue.tf                # Glue Database & Tables
│   ├── aws_redshift.tf            # Redshift Serverless
│   ├── aws_redshift_data.tf       # Redshift DDL/DML実行
│   ├── databricks_credentials.tf  # Service / Storage Credentials
│   ├── databricks_external.tf     # External Location
│   ├── databricks_connection.tf   # Connections (Glue, Redshift)
│   ├── databricks_catalog.tf      # Foreign Catalogs
│   ├── aws_lakeformation.tf      # Lake Formation権限 (IAM_ALLOWED_PRINCIPALS)
│   ├── data/
│   │   ├── sensors.csv            # センサーマスタ (20行)
│   │   └── machines.csv           # 機械マスタ (10行)
│   └── sql/
│       ├── create_sensor_readings.sql
│       ├── create_production_events.sql
│       ├── insert_sensor_readings.sql
│       └── insert_production_events.sql
├── notebooks/
│   └── federation_demo.sql        # デモクエリノートブック
├── .gitignore
└── README.md
```
