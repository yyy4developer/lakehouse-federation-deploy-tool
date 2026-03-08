-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lakehouse Federation デモ (Azure)
-- MAGIC ## Unity Catalog による外部データソースの統合ガバナンス
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC | 章 | テーマ | 内容 |
-- MAGIC |----|--------|------|
-- MAGIC | **第1章** | メタデータ統合 | 5 ソースを Lakehouse Federation でクエリ |
-- MAGIC | **第2章** | クロスソース分析 | 複数ソースを JOIN してリネージを可視化 |
-- MAGIC | **第3章** | アクセス制御 | Federation カタログへの権限管理 |
-- MAGIC | **第4章** | AI 活用 | Genie で自然言語データ探索 |
-- MAGIC
-- MAGIC **接続ソース**: Amazon Redshift / PostgreSQL (Azure Flexible Server) / Azure Synapse / Google BigQuery

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 設定

-- COMMAND ----------

DECLARE OR REPLACE query_prefix STRING DEFAULT 'lhf_xb5v_demo_query';
DECLARE OR REPLACE catalog_prefix STRING DEFAULT 'lhf_xb5v_demo_catalog';
DECLARE OR REPLACE db_prefix STRING DEFAULT 'lhf_xb5v_demo';
DECLARE OR REPLACE analysis_catalog STRING DEFAULT 'lhf_xb5v_demo_union_dbx';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第1章: メタデータ統合 — Lakehouse Federation
-- MAGIC
-- MAGIC ```
-- MAGIC ┌──────────────────────────────────────────────────────────────────────────────┐
-- MAGIC │                          Databricks Unity Catalog                            │
-- MAGIC │                                                                              │
-- MAGIC │                           Query Federation                                   │
-- MAGIC │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                        │
-- MAGIC │  │ Redshift │ │ Postgres │ │ Synapse  │ │ BigQuery │                        │
-- MAGIC │  │  (JDBC)  │ │  (JDBC)  │ │  (JDBC)  │ │  (JDBC)  │                        │
-- MAGIC │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘                        │
-- MAGIC └───────┼────────────┼────────────┼────────────┼──────────────────────────────┘
-- MAGIC         │            │            │            │
-- MAGIC   ┌─────▼──────┐ ┌──▼───────┐ ┌──▼──────┐ ┌──▼──────┐
-- MAGIC   │  Redshift  │ │  Azure   │ │ Synapse │ │ BigQuery│
-- MAGIC   │  (AWS)     │ │ Postgres │ │ (Azure) │ │  (GCP)  │
-- MAGIC   └────────────┘ └──────────┘ └─────────┘ └─────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC | 方式 | 対象 | 仕組み |
-- MAGIC |------|------|--------|
-- MAGIC | **Query Federation** | Redshift, PostgreSQL, Synapse, BigQuery | JDBC 経由でクエリをプッシュダウン |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.1 Query Federation: Amazon Redshift
-- MAGIC
-- MAGIC Redshift Serverless に JDBC 経由でクエリを発行。
-- MAGIC フィルタや集約は **Redshift 側にプッシュダウン** され、結果のみが返却されます。

-- COMMAND ----------

-- センサー読取値
SELECT * FROM IDENTIFIER(query_prefix || '_redshift.' || db_prefix || '.sensor_readings');

-- COMMAND ----------

-- 生産イベント
SELECT * FROM IDENTIFIER(query_prefix || '_redshift.' || db_prefix || '.production_events');

-- COMMAND ----------

-- 品質検査
SELECT * FROM IDENTIFIER(query_prefix || '_redshift.' || db_prefix || '.quality_inspections');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.2 Query Federation: PostgreSQL
-- MAGIC
-- MAGIC PostgreSQL (Azure Flexible Server) に JDBC 経由でクエリを発行。
-- MAGIC 機械マスタ、保守ログ、作業指示書など運用系データを参照します。

-- COMMAND ----------

-- 機械マスタ
SELECT * FROM IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.machines');

-- COMMAND ----------

-- 保守ログ
SELECT * FROM IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.maintenance_logs');

-- COMMAND ----------

-- 作業指示書
SELECT * FROM IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.work_orders');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.3 Query Federation: Azure Synapse
-- MAGIC
-- MAGIC Azure Synapse Analytics (Serverless SQL Pool) に JDBC 経由でクエリを発行。
-- MAGIC シフト管理やエネルギー消費データを参照します。

-- COMMAND ----------

-- シフトスケジュール
SELECT * FROM IDENTIFIER(query_prefix || '_synapse.' || db_prefix || '.shift_schedules');

-- COMMAND ----------

-- 電力消費量
SELECT * FROM IDENTIFIER(query_prefix || '_synapse.' || db_prefix || '.energy_consumption');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.4 Query Federation: Google BigQuery
-- MAGIC
-- MAGIC Google BigQuery に JDBC 経由でクエリを発行。
-- MAGIC 稼働停止記録やコスト配分データを参照します。

-- COMMAND ----------

-- 稼働停止記録
SELECT * FROM IDENTIFIER(query_prefix || '_bigquery.' || db_prefix || '_factory.downtime_records');

-- COMMAND ----------

-- コスト配分
SELECT * FROM IDENTIFIER(query_prefix || '_bigquery.' || db_prefix || '_factory.cost_allocation');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第2章: クロスソース分析とリネージ
-- MAGIC
-- MAGIC 複数の Federation ソースを **クロスソース JOIN** して分析テーブルを作成し、
-- MAGIC Unity Catalog の **Lineage** 機能で依存関係を可視化します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.1 分析スキーマの準備

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(analysis_catalog || '.lhf_demo');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.2 機械ヘルスサマリーの作成
-- MAGIC
-- MAGIC PostgreSQL (マスタ) + Redshift (トランザクション) をクロスソース JOIN し、
-- MAGIC 機械ごとの総合ヘルスサマリーを作成します。

-- COMMAND ----------

EXECUTE IMMEDIATE
'CREATE OR REPLACE TABLE ' || analysis_catalog || '.lhf_demo.machine_health_summary AS
WITH sensor_summary AS (
  SELECT
    r.machine_id,
    COUNT(CASE WHEN r.status = \'warning\' THEN 1 END) AS sensor_warnings,
    COUNT(CASE WHEN r.status = \'critical\' THEN 1 END) AS sensor_criticals
  FROM ' || query_prefix || '_redshift.' || db_prefix || '.sensor_readings r
  GROUP BY r.machine_id
),
event_summary AS (
  SELECT
    e.machine_id,
    COUNT(CASE WHEN e.event_type = \'error\' THEN 1 END) AS error_count,
    SUM(CASE WHEN e.event_type = \'maintenance\' THEN e.duration_minutes ELSE 0 END) AS maintenance_minutes
  FROM ' || query_prefix || '_redshift.' || db_prefix || '.production_events e
  GROUP BY e.machine_id
),
quality_agg AS (
  SELECT
    machine_id,
    COUNT(*) AS total_inspections,
    COUNT(CASE WHEN result = \'pass\' THEN 1 END) AS passed_inspections,
    COUNT(CASE WHEN result = \'fail\' THEN 1 END) AS failed_inspections,
    SUM(defect_count) AS total_defects
  FROM ' || query_prefix || '_redshift.' || db_prefix || '.quality_inspections
  GROUP BY machine_id
)
SELECT
  m.machine_id, m.machine_name, m.production_line, m.factory,
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
FROM ' || query_prefix || '_postgres.' || db_prefix || '.machines m
LEFT JOIN sensor_summary ss ON m.machine_id = ss.machine_id
LEFT JOIN event_summary es ON m.machine_id = es.machine_id
LEFT JOIN quality_agg qa ON m.machine_id = qa.machine_id';

-- COMMAND ----------

SELECT * FROM IDENTIFIER(analysis_catalog || '.lhf_demo.machine_health_summary')
ORDER BY sensor_critical_count DESC, error_event_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.3 追加ソースのクロスソース JOIN

-- COMMAND ----------

-- PostgreSQL: 保守履歴の統合
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(ml.log_id) AS maintenance_log_count,
  COUNT(wo.order_id) AS work_order_count,
  COUNT(CASE WHEN wo.status = 'open' THEN 1 END) AS open_work_orders
FROM IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.machines') m
LEFT JOIN IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.maintenance_logs') ml ON m.machine_id = ml.machine_id
LEFT JOIN IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.work_orders') wo ON m.machine_id = wo.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY open_work_orders DESC;

-- COMMAND ----------

-- Synapse: シフト・エネルギーの統合
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(DISTINCT ss.shift_id) AS total_shifts,
  ROUND(SUM(ec.kwh_consumed), 2) AS total_kwh,
  ROUND(SUM(ec.cost_usd), 2) AS total_energy_cost_usd
FROM IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.machines') m
LEFT JOIN IDENTIFIER(query_prefix || '_synapse.' || db_prefix || '.shift_schedules') ss ON m.machine_id = ss.machine_id
LEFT JOIN IDENTIFIER(query_prefix || '_synapse.' || db_prefix || '.energy_consumption') ec ON m.machine_id = ec.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY total_energy_cost_usd DESC;

-- COMMAND ----------

-- BigQuery: 稼働停止・コスト分析
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(dr.record_id) AS downtime_incidents,
  ROUND(SUM(ca.amount_usd), 2) AS total_allocated_cost_usd
FROM IDENTIFIER(query_prefix || '_postgres.' || db_prefix || '.machines') m
LEFT JOIN IDENTIFIER(query_prefix || '_bigquery.' || db_prefix || '_factory.downtime_records') dr ON m.machine_id = dr.machine_id
LEFT JOIN IDENTIFIER(query_prefix || '_bigquery.' || db_prefix || '_factory.cost_allocation') ca ON m.machine_id = ca.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY downtime_incidents DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.4 全ソース統合テーブル (Factory Operations Union)
-- MAGIC
-- MAGIC 全 5 ソース (Redshift + PostgreSQL + Synapse + BigQuery) のデータを
-- MAGIC `machine_id` で統合し、1 つのテーブルに集約します。

-- COMMAND ----------

EXECUTE IMMEDIATE
'CREATE OR REPLACE TABLE ' || analysis_catalog || '.lhf_demo.factory_operations_union AS
WITH machines_base AS (
  SELECT
    m.machine_id, m.machine_name, m.production_line, m.factory, m.status AS machine_status
  FROM ' || query_prefix || '_postgres.' || db_prefix || '.machines m
),
redshift_sensors AS (
  SELECT machine_id,
    COUNT(*) AS sensor_reading_count,
    COUNT(CASE WHEN status = \'warning\' THEN 1 END) AS sensor_warnings,
    COUNT(CASE WHEN status = \'critical\' THEN 1 END) AS sensor_criticals
  FROM ' || query_prefix || '_redshift.' || db_prefix || '.sensor_readings
  GROUP BY machine_id
),
redshift_events AS (
  SELECT machine_id,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN event_type = \'error\' THEN 1 END) AS error_count
  FROM ' || query_prefix || '_redshift.' || db_prefix || '.production_events
  GROUP BY machine_id
),
redshift_quality AS (
  SELECT machine_id,
    COUNT(*) AS inspection_count,
    SUM(defect_count) AS defect_count
  FROM ' || query_prefix || '_redshift.' || db_prefix || '.quality_inspections
  GROUP BY machine_id
),
postgres_maint AS (
  SELECT machine_id, COUNT(*) AS maintenance_log_count
  FROM ' || query_prefix || '_postgres.' || db_prefix || '.maintenance_logs
  GROUP BY machine_id
),
postgres_wo AS (
  SELECT machine_id,
    COUNT(*) AS work_order_count,
    COUNT(CASE WHEN status = \'open\' THEN 1 END) AS open_work_orders
  FROM ' || query_prefix || '_postgres.' || db_prefix || '.work_orders
  GROUP BY machine_id
),
synapse_shifts AS (
  SELECT machine_id, COUNT(*) AS shift_count, ROUND(SUM(hours_worked), 1) AS total_hours_worked
  FROM ' || query_prefix || '_synapse.' || db_prefix || '.shift_schedules
  GROUP BY machine_id
),
synapse_energy AS (
  SELECT machine_id, ROUND(SUM(kwh_consumed), 2) AS total_kwh, ROUND(SUM(cost_usd), 2) AS energy_cost_usd
  FROM ' || query_prefix || '_synapse.' || db_prefix || '.energy_consumption
  GROUP BY machine_id
),
bq_downtime AS (
  SELECT machine_id, COUNT(*) AS downtime_incidents
  FROM ' || query_prefix || '_bigquery.' || db_prefix || '_factory.downtime_records
  GROUP BY machine_id
),
bq_cost AS (
  SELECT machine_id, ROUND(SUM(amount_usd), 2) AS allocated_cost_usd
  FROM ' || query_prefix || '_bigquery.' || db_prefix || '_factory.cost_allocation
  GROUP BY machine_id
)
SELECT
  mb.machine_id, mb.machine_name, mb.production_line, mb.factory, mb.machine_status,
  COALESCE(rs.sensor_reading_count, 0) AS sensor_reading_count,
  COALESCE(rs.sensor_warnings, 0) AS sensor_warnings,
  COALESCE(rs.sensor_criticals, 0) AS sensor_criticals,
  COALESCE(re.event_count, 0) AS production_event_count,
  COALESCE(re.error_count, 0) AS error_event_count,
  COALESCE(rq.inspection_count, 0) AS inspection_count,
  COALESCE(rq.defect_count, 0) AS defect_count,
  COALESCE(pm.maintenance_log_count, 0) AS maintenance_log_count,
  COALESCE(pw.work_order_count, 0) AS work_order_count,
  COALESCE(pw.open_work_orders, 0) AS open_work_orders,
  COALESCE(ss.shift_count, 0) AS shift_count,
  COALESCE(ss.total_hours_worked, 0) AS total_hours_worked,
  COALESCE(se.total_kwh, 0) AS total_energy_kwh,
  COALESCE(se.energy_cost_usd, 0) AS energy_cost_usd,
  COALESCE(bd.downtime_incidents, 0) AS downtime_incidents,
  COALESCE(bc.allocated_cost_usd, 0) AS allocated_cost_usd
FROM machines_base mb
LEFT JOIN redshift_sensors rs ON mb.machine_id = rs.machine_id
LEFT JOIN redshift_events re ON mb.machine_id = re.machine_id
LEFT JOIN redshift_quality rq ON mb.machine_id = rq.machine_id
LEFT JOIN postgres_maint pm ON mb.machine_id = pm.machine_id
LEFT JOIN postgres_wo pw ON mb.machine_id = pw.machine_id
LEFT JOIN synapse_shifts ss ON mb.machine_id = ss.machine_id
LEFT JOIN synapse_energy se ON mb.machine_id = se.machine_id
LEFT JOIN bq_downtime bd ON mb.machine_id = bd.machine_id
LEFT JOIN bq_cost bc ON mb.machine_id = bc.machine_id';

-- COMMAND ----------

SELECT * FROM IDENTIFIER(analysis_catalog || '.lhf_demo.factory_operations_union')
ORDER BY machine_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.5 リネージの確認（UI で実施）
-- MAGIC
-- MAGIC 1. **Catalog Explorer** を開く（左サイドバー → Catalog）
-- MAGIC 2. 分析カタログ → `lhf_demo` → `factory_operations_union` を選択
-- MAGIC 3. **Lineage** タブをクリック
-- MAGIC
-- MAGIC ```
-- MAGIC  Amazon Redshift ───┐
-- MAGIC  PostgreSQL ────────┤
-- MAGIC  Azure Synapse ─────┼──▶ factory_operations_union
-- MAGIC  Google BigQuery ───┘
-- MAGIC ```
-- MAGIC
-- MAGIC > **ポイント**: 5 つの外部ソースからデータが統合されている全体像が一目でわかります。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第3章: アクセス制御
-- MAGIC
-- MAGIC Unity Catalog は Federation カタログに対しても **同じ権限管理モデル** を適用します。
-- MAGIC
-- MAGIC | 特徴 | 説明 |
-- MAGIC |------|------|
-- MAGIC | **階層型アクセス制御** | カタログ → スキーマ → テーブルの階層で権限を継承 |
-- MAGIC | **GRANT / REVOKE** | 標準 SQL でユーザー・グループに権限を付与・取消 |
-- MAGIC | **行・列フィルタ** | 行レベル・列レベルのアクセス制御が可能 |
-- MAGIC | **外部テーブル対応** | Federation 経由の外部テーブルにも同じ権限モデルが適用 |
-- MAGIC | **監査ログ** | 誰が・いつ・何にアクセスしたかを自動記録 |

-- COMMAND ----------

-- Federation カタログの権限一覧を確認
EXECUTE IMMEDIATE 'SHOW GRANTS ON CATALOG ' || query_prefix || '_redshift';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 権限管理の例
-- MAGIC
-- MAGIC 以下は権限管理の SQL 例です。実行する場合はコメントを外してください。

-- COMMAND ----------

-- カタログへのアクセス権付与
-- GRANT USE CATALOG ON CATALOG <catalog_name> TO `data_team`;

-- テーブルへの SELECT 権限付与
-- GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `analyst`;

-- 権限の取消
-- REVOKE SELECT ON TABLE <catalog>.<schema>.<table> FROM `analyst`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第4章: AI 活用 — Genie
-- MAGIC
-- MAGIC **Genie** は自然言語でデータに質問し、SQL を自動生成・実行する AI 機能です。
-- MAGIC
-- MAGIC ### Genie Space の作成手順（UI で実施）
-- MAGIC
-- MAGIC 1. 左サイドバー → **Genie** → **New**
-- MAGIC 2. 以下を設定:
-- MAGIC    - **タイトル**: `工場データ分析`
-- MAGIC    - **テーブル**: 分析カタログの `lhf_demo.factory_operations_union`
-- MAGIC 3. **保存**
-- MAGIC
-- MAGIC ### 質問例
-- MAGIC
-- MAGIC | 質問 | 期待される分析 |
-- MAGIC |------|---------------|
-- MAGIC | 一番異常が多い機械は？ | sensor_criticals + error_event_count の集計 |
-- MAGIC | 製造ラインごとの品質合格率は？ | production_line 別の quality_pass_rate_pct |
-- MAGIC | メンテナンス時間が最も長い機械は？ | total_maintenance_minutes の降順 |
-- MAGIC | エネルギーコストが最も高い機械は？ | energy_cost_usd の降順 |
-- MAGIC | A棟の機械の稼働状況を教えて | factory = 'A棟' のフィルタリング |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # まとめ
-- MAGIC
-- MAGIC | 章 | 実演内容 |
-- MAGIC |----|---------|
-- MAGIC | **第1章** | 5 ソース (Redshift, PostgreSQL, Synapse, BigQuery) を Federation で統合 |
-- MAGIC | **第2章** | クロスソース JOIN → `factory_operations_union` 作成 → リネージ可視化 |
-- MAGIC | **第3章** | Federation カタログへの GRANT / REVOKE によるアクセス制御 |
-- MAGIC | **第4章** | Genie で自然言語データ探索 |
-- MAGIC
-- MAGIC **Unity Catalog による統合データガバナンス**:
-- MAGIC - データを移動せずに外部ソースを一元管理
-- MAGIC - マルチクラウド (AWS / Azure / GCP) 対応
-- MAGIC - Discover / Lineage でデータ資産の全体像を把握
-- MAGIC - 外部テーブルを含めた一元的なアクセス制御
-- MAGIC - メタデータを活かした AI データ探索
