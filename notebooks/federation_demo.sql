-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 工場データ統合デモ
-- MAGIC # Lakehouse Federation / Discover / Lineage / Genie
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC | 章 | テーマ | 内容 |
-- MAGIC |----|--------|------|
-- MAGIC | **第1章** | Lakehouse Federation | 外部データソース（Glue / Redshift）への接続・クエリ |
-- MAGIC | **第2章** | Discover | Unity Catalogのデータ検索・探索機能の紹介 |
-- MAGIC | **第3章** | Lineage | Federationデータから新テーブルを作成し、データリネージを確認 |
-- MAGIC | **第4章** | Genie | AI搭載のGenie Spaceによる自然言語データ探索 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第1章: Lakehouse Federation
-- MAGIC
-- MAGIC ## アーキテクチャ概要
-- MAGIC
-- MAGIC ```
-- MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
-- MAGIC │                        Databricks Unity Catalog                            │
-- MAGIC │                                                                             │
-- MAGIC │   ┌─────────────────────────┐       ┌─────────────────────────┐            │
-- MAGIC │   │  glue_factory カタログ   │       │ redshift_factory カタログ│            │
-- MAGIC │   │  (Catalog Federation)    │       │  (Query Federation)     │            │
-- MAGIC │   └────────────┬────────────┘       └────────────┬────────────┘            │
-- MAGIC │                │                                  │                         │
-- MAGIC └────────────────┼──────────────────────────────────┼─────────────────────────┘
-- MAGIC                  │                                  │
-- MAGIC        ┌─────────▼─────────┐              ┌────────▼────────┐
-- MAGIC        │   AWS Glue        │              │ Amazon Redshift  │
-- MAGIC        │  Data Catalog     │              │   Serverless     │
-- MAGIC        │  (HMS互換)        │              │   (JDBC接続)     │
-- MAGIC        └─────────┬─────────┘              └─────────────────┘
-- MAGIC                  │
-- MAGIC        ┌─────────▼─────────┐
-- MAGIC        │    Amazon S3      │
-- MAGIC        │  ┌─────┐┌──────┐ │
-- MAGIC        │  │Parq.││Delta │ │
-- MAGIC        │  └─────┘└──────┘ │
-- MAGIC        │  ┌───────────┐   │
-- MAGIC        │  │  Iceberg  │   │
-- MAGIC        │  └───────────┘   │
-- MAGIC        └───────────────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC ## Catalog Federation と Query Federation の違い
-- MAGIC
-- MAGIC | 項目 | Catalog Federation（Glue） | Query Federation（Redshift） |
-- MAGIC |------|---------------------------|------------------------------|
-- MAGIC | **接続方式** | Glue APIでメタデータ取得 → S3から直接データ読取 | JDBC経由でクエリをプッシュダウン |
-- MAGIC | **データの流れ** | S3 → Databricksが直接読取（Spark実行） | Redshiftがクエリ実行 → 結果をDatabricksに返却 |
-- MAGIC | **対応フォーマット** | Parquet, Delta, Iceberg, CSV, JSON等 | Redshift内部テーブル |
-- MAGIC | **パフォーマンス特性** | 大量データのスキャンに強い（Sparkの並列処理） | フィルタ・集約のプッシュダウンで効率的 |
-- MAGIC | **ユースケース** | データレイク上のマスタデータ・履歴データ | DWH上のトランザクションデータ・リアルタイム集計 |
-- MAGIC
-- MAGIC ### 本デモのデータ構成
-- MAGIC
-- MAGIC | カタログ | テーブル | フォーマット | データ種別 | 行数 |
-- MAGIC |---------|---------|------------|-----------|------|
-- MAGIC | `glue_factory` | `sensors` | **Parquet** | センサーマスタ | 20行 |
-- MAGIC | `glue_factory` | `machines` | **Delta** | 機械マスタ | 10行 |
-- MAGIC | `glue_factory` | `quality_inspections` | **Iceberg** | 品質検査 | 50行 |
-- MAGIC | `redshift_factory` | `sensor_readings` | - | センサー読取値 | 100行 |
-- MAGIC | `redshift_factory` | `production_events` | - | 生産イベント | 30行 |
-- MAGIC | `redshift_factory` | `quality_inspections` | - | 品質検査 | 40行 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.1 Catalog Federation: AWS Glueカタログ
-- MAGIC
-- MAGIC Glue Data Catalogに登録されたテーブルを、Unity Catalog経由で透過的に参照します。
-- MAGIC Databricksは **S3上のデータを直接読み取り** 、Sparkエンジンで処理します。

-- COMMAND ----------

-- Glue外部カタログ内のスキーマ一覧
SHOW SCHEMAS IN glue_factory;

-- COMMAND ----------

-- テーブル一覧
SHOW TABLES IN glue_factory.lhf_demo_factory_master;

-- COMMAND ----------

-- sensorsテーブル（Parquet）のメタデータ確認
DESCRIBE TABLE EXTENDED glue_factory.lhf_demo_factory_master.sensors;

-- COMMAND ----------

-- センサーマスタデータ（Parquet）
SELECT * FROM glue_factory.lhf_demo_factory_master.sensors;

-- COMMAND ----------

-- machinesテーブル（Delta）のメタデータ確認
DESCRIBE TABLE EXTENDED glue_factory.lhf_demo_factory_master.machines;

-- COMMAND ----------

-- 機械マスタデータ（Delta）
SELECT * FROM glue_factory.lhf_demo_factory_master.machines;

-- COMMAND ----------

-- quality_inspectionsテーブル（Iceberg）のメタデータ確認
DESCRIBE TABLE EXTENDED glue_factory.lhf_demo_factory_master.quality_inspections;

-- COMMAND ----------

-- 品質検査データ（Iceberg）
SELECT * FROM glue_factory.lhf_demo_factory_master.quality_inspections
ORDER BY inspection_time DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.2 Query Federation: Amazon Redshiftカタログ
-- MAGIC
-- MAGIC Redshift Serverlessに対してJDBC経由でクエリを発行します。
-- MAGIC フィルタや集約は **Redshift側にプッシュダウン** され、結果のみがDatabricksに返却されます。

-- COMMAND ----------

-- Redshift外部カタログ内のスキーマ一覧
SHOW SCHEMAS IN redshift_factory;

-- COMMAND ----------

-- テーブル一覧
SHOW TABLES IN redshift_factory.public;

-- COMMAND ----------

-- sensor_readingsのメタデータ確認
DESCRIBE TABLE EXTENDED redshift_factory.public.sensor_readings;

-- COMMAND ----------

-- センサー読取データ
SELECT * FROM redshift_factory.public.sensor_readings
ORDER BY reading_time DESC
LIMIT 20;

-- COMMAND ----------

-- production_eventsのメタデータ確認
DESCRIBE TABLE EXTENDED redshift_factory.public.production_events;

-- COMMAND ----------

-- 生産イベントデータ
SELECT * FROM redshift_factory.public.production_events
ORDER BY event_time DESC
LIMIT 20;

-- COMMAND ----------

-- quality_inspectionsのメタデータ確認
DESCRIBE TABLE EXTENDED redshift_factory.public.quality_inspections;

-- COMMAND ----------

-- 品質検査データ
SELECT * FROM redshift_factory.public.quality_inspections
ORDER BY inspection_time DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第2章: Discover（データ検索・探索）
-- MAGIC
-- MAGIC ## Discover機能とは
-- MAGIC
-- MAGIC Unity Catalogの **Discover** 機能は、組織内のすべてのデータ資産を
-- MAGIC 検索・閲覧・理解するためのデータカタログUIです。
-- MAGIC
-- MAGIC ### 主な機能
-- MAGIC
-- MAGIC | 機能 | 説明 |
-- MAGIC |------|------|
-- MAGIC | **全文検索** | テーブル名・カラム名・説明文をキーワードで横断検索 |
-- MAGIC | **フィルタリング** | カタログ・スキーマ・タグ・オーナーで絞り込み |
-- MAGIC | **メタデータ閲覧** | テーブル説明・カラムコメント・スキーマ情報を一覧表示 |
-- MAGIC | **データプレビュー** | テーブルのサンプルデータをUI上で即座に確認 |
-- MAGIC | **AI生成ドキュメント** | テーブル・カラムの説明をAIが自動生成（提案） |
-- MAGIC | **人気度表示** | よくアクセスされるテーブルのランキング表示 |
-- MAGIC
-- MAGIC ### デモ手順（UIで実施）
-- MAGIC
-- MAGIC 1. **Catalog Explorer** を開く（左サイドバー → Catalog）
-- MAGIC 2. 検索バーで `sensors` や `machines` を検索
-- MAGIC    - Federation経由のGlueテーブルとRedshiftテーブルが両方ヒットすることを確認
-- MAGIC 3. `glue_factory.lhf_demo_factory_master.sensors` を選択
-- MAGIC    - **テーブル説明**（日本語）が表示されることを確認
-- MAGIC    - **カラムコメント**（日本語）が各カラムに表示されることを確認
-- MAGIC    - **スキーマ**タブでデータ型・コメントを一覧確認
-- MAGIC    - **サンプルデータ**タブでプレビューを確認
-- MAGIC 4. `glue_factory.lhf_demo_factory_master.machines`（Delta）で同様に確認
-- MAGIC 5. `redshift_factory.public.sensor_readings` で、Redshiftテーブルのメタデータも確認
-- MAGIC
-- MAGIC > **ポイント**: Federationで接続した外部テーブルも、Unity Catalog管理のテーブルと
-- MAGIC > 同じUIで検索・閲覧できます。データの所在を意識せず、統一的にデータを発見できます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第3章: Lineage（データリネージ）
-- MAGIC
-- MAGIC ## Lineage機能とは
-- MAGIC
-- MAGIC Unity Catalogの **Lineage** 機能は、データの流れを自動的に追跡し、
-- MAGIC テーブル間・カラム間の依存関係を可視化します。
-- MAGIC
-- MAGIC - **テーブルレベルリネージ**: どのテーブルからどのテーブルが作成されたか
-- MAGIC - **カラムレベルリネージ**: どのカラムがどのカラムに由来するか
-- MAGIC - **ノートブックリネージ**: どのノートブック/ジョブがテーブルを更新したか
-- MAGIC
-- MAGIC ### デモの流れ
-- MAGIC
-- MAGIC 1. Federationデータをクロスソース**JOIN**して分析テーブルを作成
-- MAGIC 2. UIでリネージグラフを確認 → 外部ソース（Glue/Redshift）からの依存が可視化される

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.1 分析用カタログ・スキーマの準備

-- COMMAND ----------

-- 分析結果を格納するスキーマを作成
CREATE CATALOG IF NOT EXISTS yunyi_catalog;
CREATE SCHEMA IF NOT EXISTS yunyi_catalog.lhf_demo
COMMENT '工場データ分析用スキーマ - Federationデータから生成した分析テーブルを格納';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 機械ヘルスサマリーテーブルの作成
-- MAGIC
-- MAGIC Glue（3テーブル）とRedshift（3テーブル）の **全6テーブル** をクロスソースJOINし、
-- MAGIC 機械ごとの総合ヘルスサマリーを作成します。

-- COMMAND ----------

CREATE OR REPLACE TABLE yunyi_catalog.lhf_demo.machine_health_summary
COMMENT '機械別総合ヘルスサマリー - Glue（Parquet/Delta/Iceberg）とRedshift（3テーブル）をJOINして生成'
AS
WITH sensor_summary AS (
  SELECT
    r.machine_id,
    COUNT(CASE WHEN r.status = 'warning' THEN 1 END) AS sensor_warnings,
    COUNT(CASE WHEN r.status = 'critical' THEN 1 END) AS sensor_criticals
  FROM redshift_factory.public.sensor_readings r
  GROUP BY r.machine_id
),
event_summary AS (
  SELECT
    e.machine_id,
    COUNT(CASE WHEN e.event_type = 'error' THEN 1 END) AS error_count,
    SUM(CASE WHEN e.event_type = 'maintenance' THEN e.duration_minutes ELSE 0 END) AS maintenance_minutes
  FROM redshift_factory.public.production_events e
  GROUP BY e.machine_id
),
quality_all AS (
  SELECT machine_id, result, defect_count FROM glue_factory.lhf_demo_factory_master.quality_inspections
  UNION ALL
  SELECT machine_id, result, defect_count FROM redshift_factory.public.quality_inspections
),
quality_agg AS (
  SELECT
    machine_id,
    COUNT(*) AS total_inspections,
    COUNT(CASE WHEN result = 'pass' THEN 1 END) AS passed_inspections,
    COUNT(CASE WHEN result = 'fail' THEN 1 END) AS failed_inspections,
    SUM(defect_count) AS total_defects
  FROM quality_all
  GROUP BY machine_id
)
SELECT
  m.machine_id,
  m.machine_name,
  m.production_line,
  m.factory,
  m.status AS machine_status,
  COALESCE(ss.sensor_warnings, 0) AS sensor_warning_count,
  COALESCE(ss.sensor_criticals, 0) AS sensor_critical_count,
  COALESCE(es.error_count, 0) AS error_event_count,
  COALESCE(es.maintenance_minutes, 0) AS total_maintenance_minutes,
  COALESCE(qa.total_inspections, 0) AS total_inspection_count,
  COALESCE(qa.passed_inspections, 0) AS passed_inspection_count,
  COALESCE(qa.failed_inspections, 0) AS failed_inspection_count,
  COALESCE(qa.total_defects, 0) AS total_defect_count,
  ROUND(COALESCE(qa.passed_inspections, 0) * 100.0 / NULLIF(qa.total_inspections, 0), 1) AS quality_pass_rate_pct
FROM glue_factory.lhf_demo_factory_master.machines m
LEFT JOIN sensor_summary ss ON m.machine_id = ss.machine_id
LEFT JOIN event_summary es ON m.machine_id = es.machine_id
LEFT JOIN quality_agg qa ON m.machine_id = qa.machine_id;

-- COMMAND ----------

-- 作成したテーブルの確認
SELECT * FROM yunyi_catalog.lhf_demo.machine_health_summary
ORDER BY sensor_critical_count DESC, error_event_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.3 異常センサー読取詳細テーブルの作成
-- MAGIC
-- MAGIC Glueのセンサー・機械マスタ（Parquet + Delta）とRedshiftの読取値をクロスソースJOINし、
-- MAGIC 異常読取値のみを抽出した分析テーブルを作成します。

-- COMMAND ----------

CREATE OR REPLACE TABLE yunyi_catalog.lhf_demo.abnormal_sensor_readings
COMMENT '異常センサー読取詳細 - Glueセンサー/機械マスタとRedshift読取値をJOINし、警告・危険レベルのみ抽出'
AS
SELECT
  m.machine_name,
  m.production_line,
  m.factory,
  s.sensor_name,
  s.sensor_type,
  s.unit,
  r.reading_time,
  r.value AS reading_value,
  r.status AS reading_status
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.sensor_readings r
  ON m.machine_id = r.machine_id
JOIN glue_factory.lhf_demo_factory_master.sensors s
  ON r.sensor_id = s.sensor_id
WHERE r.status != 'normal';

-- COMMAND ----------

-- 作成したテーブルの確認
SELECT * FROM yunyi_catalog.lhf_demo.abnormal_sensor_readings
ORDER BY production_line, reading_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.4 リネージの確認（UIで実施）
-- MAGIC
-- MAGIC 作成した2つの分析テーブルのリネージをCatalog Explorer UIで確認します。
-- MAGIC
-- MAGIC #### 手順
-- MAGIC
-- MAGIC 1. **Catalog Explorer** を開く（左サイドバー → Catalog）
-- MAGIC 2. `yunyi_catalog` → `lhf_demo` → `machine_health_summary` を選択
-- MAGIC 3. **Lineage** タブをクリック
-- MAGIC 4. 以下のリネージグラフが表示されることを確認:
-- MAGIC
-- MAGIC ```
-- MAGIC ┌──────────────────────────────────┐
-- MAGIC │ glue_factory                     │
-- MAGIC │  ├─ sensors          (Parquet)   │──┐
-- MAGIC │  ├─ machines         (Delta)     │──┤
-- MAGIC │  └─ quality_inspections (Iceberg)│──┤
-- MAGIC └──────────────────────────────────┘  │
-- MAGIC                                       ├──▶ yunyi_catalog.lhf_demo
-- MAGIC ┌──────────────────────────────────┐  │      .machine_health_summary
-- MAGIC │ redshift_factory                 │  │
-- MAGIC │  ├─ sensor_readings              │──┤
-- MAGIC │  ├─ production_events            │──┤
-- MAGIC │  └─ quality_inspections          │──┘
-- MAGIC └──────────────────────────────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC > **ポイント**: Federation経由の外部テーブル（Glue/Redshift）からの
-- MAGIC > データの流れが、Unity Catalogのリネージグラフで自動的に追跡されます。
-- MAGIC > これにより、外部データソースを含めたデータガバナンスが実現できます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第4章: Genie（AI搭載データ探索）
-- MAGIC
-- MAGIC ## Genie Spaceとは
-- MAGIC
-- MAGIC **Genie** は、Databricksに搭載されたAI機能で、
-- MAGIC 自然言語でデータに対する質問を行い、自動的にSQLクエリを生成・実行します。
-- MAGIC
-- MAGIC ユーザーはSQLの知識がなくても、日本語で質問するだけで
-- MAGIC データの探索・分析が可能になります。
-- MAGIC
-- MAGIC ### Genie Spaceの作成手順（UIで実施）
-- MAGIC
-- MAGIC 1. **左サイドバー** → **Genie** をクリック
-- MAGIC 2. **「New」** をクリックしてGenie Spaceを新規作成
-- MAGIC 3. 以下の情報を設定:
-- MAGIC    - **タイトル**: `工場データ分析`
-- MAGIC    - **説明**: `工場のセンサー、機械、品質検査データを探索するためのGenie Space`
-- MAGIC    - **テーブル**:
-- MAGIC      - `yunyi_catalog.lhf_demo.machine_health_summary`
-- MAGIC      - `yunyi_catalog.lhf_demo.abnormal_sensor_readings`
-- MAGIC 4. **保存** をクリック
-- MAGIC
-- MAGIC ### 質問例
-- MAGIC
-- MAGIC Genie Spaceが作成されたら、以下のような自然言語の質問を試してみてください:
-- MAGIC
-- MAGIC | 質問例 | 期待される分析 |
-- MAGIC |--------|---------------|
-- MAGIC | `一番異常が多い機械はどれですか？` | sensor_critical_count + error_event_countの集計 |
-- MAGIC | `製造ラインごとの品質合格率を教えて` | production_line別のquality_pass_rate_pct集計 |
-- MAGIC | `メンテナンス時間が最も長い機械は？` | total_maintenance_minutesの降順ランキング |
-- MAGIC | `温度センサーの異常読取値を見せて` | sensor_type = '温度' のabnormal readings |
-- MAGIC | `A棟の機械の稼働状況を教えて` | factory = 'A棟' のフィルタリング |
-- MAGIC
-- MAGIC > **ポイント**: Genieはテーブルスキーマとカラムコメント（日本語設定済み）を参照して、
-- MAGIC > 適切なSQLを自動生成します。メタデータの品質がGenieの回答精度に直結します。
-- MAGIC > 第1章でFederationデータに付与した日本語コメントが、ここで活用されます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # まとめ
-- MAGIC
-- MAGIC | 章 | 機能 | 実演内容 |
-- MAGIC |----|------|---------|
-- MAGIC | **第1章** | Lakehouse Federation | Glue（Catalog Federation）とRedshift（Query Federation）のデータ参照 |
-- MAGIC | **第2章** | Discover | Catalog Explorer UIでFederationテーブルのメタデータを検索・閲覧 |
-- MAGIC | **第3章** | Lineage | 外部テーブルをクロスソースJOINして新テーブル作成 → リネージで依存関係を可視化 |
-- MAGIC | **第4章** | Genie | 分析テーブルからGenie Spaceを作成 → 自然言語でデータ探索 |
-- MAGIC
-- MAGIC **Databricksの統合データガバナンス**:
-- MAGIC - 外部データソースも Unity Catalog で統一管理
-- MAGIC - メタデータ（テーブル説明・カラムコメント）が全機能で活用される
-- MAGIC - データの流れ（リネージ）が外部ソースを含めて自動追跡される
-- MAGIC - AIによるデータ探索で、SQLの知識がなくてもデータ分析が可能
