-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lakehouse Federation デモ
-- MAGIC ## Unity Catalog による外部データソースの統合ガバナンス
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC | 章 | テーマ | 内容 |
-- MAGIC |----|--------|------|
-- MAGIC | **第1章** | メタデータ統合 | Lakehouse Federation で外部データソースをクエリ |
-- MAGIC | **第2章** | クロスソース分析 | 複数ソースを JOIN してリネージを可視化 |
-- MAGIC | **第3章** | アクセス制御 | Federation カタログへの権限管理 |
-- MAGIC | **第4章** | AI 活用 | Genie で自然言語データ探索 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 設定
-- MAGIC
-- MAGIC ノートブック上部の Widget で値を変更できます。デプロイ時に自動設定済みです。

-- COMMAND ----------

CREATE WIDGET TEXT query_prefix DEFAULT 'lhf_xb5v_demo_query';
CREATE WIDGET TEXT catalog_prefix DEFAULT 'lhf_xb5v_demo_catalog';
CREATE WIDGET TEXT db_prefix DEFAULT 'lhf_xb5v_demo';
CREATE WIDGET TEXT analysis_catalog DEFAULT 'lhf_xb5v_demo_union_dbx';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第1章: メタデータ統合 — Lakehouse Federation
-- MAGIC
-- MAGIC Lakehouse Federation により、外部データソースのメタデータを Unity Catalog に統合し、
-- MAGIC **データを移動せずに** 一元管理・クエリします。
-- MAGIC
-- MAGIC ```
-- MAGIC ┌──────────────────────────────────────────────────────────────────────────────┐
-- MAGIC │                          Databricks Unity Catalog                            │
-- MAGIC │                                                                              │
-- MAGIC │  Catalog Federation                    Query Federation                      │
-- MAGIC │  ┌─────────────────┐   ┌─────────────────┐ ┌─────────────────┐              │
-- MAGIC │  │  *_catalog_glue │   │ *_query_redshift │ │ *_query_postgres│              │
-- MAGIC │  │  (S3 直接読取)  │   │ (JDBC pushdown)  │ │ (JDBC pushdown) │              │
-- MAGIC │  └────────┬────────┘   └────────┬─────────┘ └────────┬────────┘              │
-- MAGIC │           │            ┌────────┴─────────┐ ┌────────┴────────┐              │
-- MAGIC │           │            │ *_query_synapse   │ │ *_query_bigquery│              │
-- MAGIC │           │            │ (JDBC pushdown)   │ │ (JDBC pushdown) │              │
-- MAGIC │           │            └────────┬──────────┘ └────────┬────────┘              │
-- MAGIC └───────────┼─────────────────────┼──────────────────────┼─────────────────────┘
-- MAGIC             │                     │                      │
-- MAGIC    ┌────────▼──────┐   ┌─────────▼───────┐   ┌─────────▼──────┐
-- MAGIC    │ AWS Glue / S3 │   │ Redshift /       │   │ BigQuery       │
-- MAGIC    │               │   │ PostgreSQL /     │   │                │
-- MAGIC    │               │   │ Synapse (JDBC)   │   │                │
-- MAGIC    └───────────────┘   └─────────────────┘   └────────────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC | 方式 | 対象 | 仕組み |
-- MAGIC |------|------|--------|
-- MAGIC | **Catalog Federation** | Glue, OneLake | メタデータ API 経由 → ストレージ直接読取 (Spark) |
-- MAGIC | **Query Federation** | Redshift, PostgreSQL, Synapse, BigQuery | JDBC 経由でクエリをプッシュダウン |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.2 Query Federation: Amazon Redshift
-- MAGIC
-- MAGIC Redshift Serverless に JDBC 経由でクエリを発行。
-- MAGIC フィルタや集約は **Redshift 側にプッシュダウン** され、結果のみが返却されます。

-- COMMAND ----------

-- センサー読取値
SELECT * FROM ${query_prefix}_redshift.${db_prefix}.sensor_readings;

-- COMMAND ----------

-- 生産イベント
SELECT * FROM ${query_prefix}_redshift.${db_prefix}.production_events;

-- COMMAND ----------

-- 品質検査
SELECT * FROM ${query_prefix}_redshift.${db_prefix}.quality_inspections;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.3 Query Federation: PostgreSQL
-- MAGIC
-- MAGIC PostgreSQL (AWS RDS / Azure Flexible Server) に JDBC 経由でクエリを発行。
-- MAGIC 保守ログや作業指示書など運用系データを参照します。

-- COMMAND ----------

-- 保守ログ
SELECT * FROM ${query_prefix}_postgres.${db_prefix}.maintenance_logs;

-- COMMAND ----------

-- 作業指示書
SELECT * FROM ${query_prefix}_postgres.${db_prefix}.work_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.4 Query Federation: Azure Synapse
-- MAGIC
-- MAGIC Azure Synapse Analytics (Serverless SQL Pool) に JDBC 経由でクエリを発行。
-- MAGIC シフト管理やエネルギー消費データを参照します。

-- COMMAND ----------

-- シフトスケジュール
SELECT * FROM ${query_prefix}_synapse.${db_prefix}.shift_schedules;

-- COMMAND ----------

-- 電力消費量
SELECT * FROM ${query_prefix}_synapse.${db_prefix}.energy_consumption;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.5 Query Federation: Google BigQuery
-- MAGIC
-- MAGIC Google BigQuery に JDBC 経由でクエリを発行。
-- MAGIC 稼働停止記録やコスト配分データを参照します。

-- COMMAND ----------

-- 稼働停止記録
SELECT * FROM ${query_prefix}_bigquery.${db_prefix}_factory.downtime_records;

-- COMMAND ----------

-- コスト配分
SELECT * FROM ${query_prefix}_bigquery.${db_prefix}_factory.cost_allocation;

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

CREATE SCHEMA IF NOT EXISTS ${analysis_catalog}.lhf_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.2 機械ヘルスサマリーの作成
-- MAGIC
-- MAGIC Glue (マスタ) + Redshift (トランザクション) をクロスソース JOIN し、
-- MAGIC 機械ごとの総合ヘルスサマリーを作成します。

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
FROM ${catalog_prefix}_glue.${db_prefix}_factory_master.machines m
LEFT JOIN ${query_prefix}_postgres.${db_prefix}.maintenance_logs ml ON m.machine_id = ml.machine_id
LEFT JOIN ${query_prefix}_postgres.${db_prefix}.work_orders wo ON m.machine_id = wo.machine_id
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
FROM ${catalog_prefix}_glue.${db_prefix}_factory_master.machines m
LEFT JOIN ${query_prefix}_synapse.${db_prefix}.shift_schedules ss ON m.machine_id = ss.machine_id
LEFT JOIN ${query_prefix}_synapse.${db_prefix}.energy_consumption ec ON m.machine_id = ec.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY total_energy_cost_usd DESC;

-- COMMAND ----------

-- BigQuery: 稼働停止・コスト分析
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(dr.record_id) AS downtime_incidents,
  ROUND(SUM(ca.amount_usd), 2) AS total_allocated_cost_usd
FROM ${catalog_prefix}_glue.${db_prefix}_factory_master.machines m
LEFT JOIN ${query_prefix}_bigquery.${db_prefix}_factory.downtime_records dr ON m.machine_id = dr.machine_id
LEFT JOIN ${query_prefix}_bigquery.${db_prefix}_factory.cost_allocation ca ON m.machine_id = ca.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY downtime_incidents DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.4 全ソース統合テーブル (Factory Operations Union)
-- MAGIC
-- MAGIC 全 5 ソースのデータを `machine_id` で統合し、1 つのテーブルに集約します。
-- MAGIC Lineage グラフで **全ソース → 1 テーブル** の統合フローが可視化されます。

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
SHOW GRANTS ON CATALOG ${query_prefix}_redshift;

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

