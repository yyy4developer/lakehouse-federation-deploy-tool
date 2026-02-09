-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lakehouse Federation Demo: Factory Sensor Data
-- MAGIC
-- MAGIC This notebook demonstrates **Databricks Lakehouse Federation** by querying data
-- MAGIC across two external data sources:
-- MAGIC
-- MAGIC | Catalog | Source | Tables | Data Type |
-- MAGIC |---------|--------|--------|-----------|
-- MAGIC | `glue_factory` | AWS Glue (S3-backed CSV) | `sensors`, `machines` | Master data |
-- MAGIC | `redshift_factory` | Amazon Redshift Serverless | `sensor_readings`, `production_events` | Transaction data |
-- MAGIC
-- MAGIC **Key demonstrations:**
-- MAGIC 1. Single-source queries on Glue and Redshift
-- MAGIC 2. Cross-source JOINs between Glue and Redshift
-- MAGIC 3. Multi-table analytics combining all 4 tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Explore Glue Catalog (Master Data)

-- COMMAND ----------

-- Show schemas in the Glue foreign catalog
SHOW SCHEMAS IN glue_factory;

-- COMMAND ----------

-- Show tables in the factory master database
SHOW TABLES IN glue_factory.lhf_demo_factory_master;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Query Glue: Sensors Master Data (20 rows)

-- COMMAND ----------

-- All sensors
SELECT * FROM glue_factory.lhf_demo_factory_master.sensors;

-- COMMAND ----------

-- Sensor count by type
SELECT
  sensor_type,
  COUNT(*) AS sensor_count,
  COLLECT_LIST(sensor_name) AS sensor_names
FROM glue_factory.lhf_demo_factory_master.sensors
GROUP BY sensor_type
ORDER BY sensor_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Query Glue: Machines Master Data (10 rows)

-- COMMAND ----------

-- All machines
SELECT * FROM glue_factory.lhf_demo_factory_master.machines;

-- COMMAND ----------

-- Machines by production line and status
SELECT
  production_line,
  factory,
  status,
  COUNT(*) AS machine_count
FROM glue_factory.lhf_demo_factory_master.machines
GROUP BY production_line, factory, status
ORDER BY production_line;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Explore Redshift Catalog (Transaction Data)

-- COMMAND ----------

SHOW SCHEMAS IN redshift_factory;

-- COMMAND ----------

SHOW TABLES IN redshift_factory.public;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Query Redshift: Sensor Readings (100 rows)

-- COMMAND ----------

-- Recent sensor readings
SELECT * FROM redshift_factory.public.sensor_readings
ORDER BY reading_time DESC
LIMIT 20;

-- COMMAND ----------

-- Reading statistics by status
SELECT
  status,
  COUNT(*) AS reading_count,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value
FROM redshift_factory.public.sensor_readings
GROUP BY status
ORDER BY reading_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Query Redshift: Production Events (30 rows)

-- COMMAND ----------

-- Recent production events
SELECT * FROM redshift_factory.public.production_events
ORDER BY event_time DESC
LIMIT 20;

-- COMMAND ----------

-- Events summary by type
SELECT
  event_type,
  COUNT(*) AS event_count,
  ROUND(AVG(duration_minutes), 1) AS avg_duration_min
FROM redshift_factory.public.production_events
GROUP BY event_type
ORDER BY event_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 7. Cross-Source JOIN: Sensors (Glue) + Readings (Redshift)
-- MAGIC
-- MAGIC Join **master data from AWS Glue** with **transaction data from Redshift**
-- MAGIC to find critical and warning sensor readings with full sensor details.

-- COMMAND ----------

SELECT
  s.sensor_name,
  s.sensor_type,
  s.unit,
  s.location,
  r.machine_id,
  r.reading_time,
  r.value,
  r.status AS reading_status
FROM glue_factory.lhf_demo_factory_master.sensors s
JOIN redshift_factory.public.sensor_readings r
  ON CAST(s.sensor_id AS INT) = r.sensor_id
WHERE r.status IN ('warning', 'critical')
ORDER BY r.reading_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Cross-Source JOIN: Machines (Glue) + Events (Redshift)
-- MAGIC
-- MAGIC Combine machine metadata with production events to analyze
-- MAGIC maintenance and error patterns per production line.

-- COMMAND ----------

SELECT
  m.machine_name,
  m.production_line,
  m.factory,
  m.status AS machine_status,
  e.event_type,
  e.event_time,
  e.duration_minutes,
  e.description
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.production_events e
  ON CAST(m.machine_id AS INT) = e.machine_id
WHERE e.event_type IN ('error', 'maintenance')
ORDER BY e.event_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. 4-Table JOIN: Full Factory Sensor Analytics
-- MAGIC
-- MAGIC Combine **all 4 tables** from both Glue and Redshift to build a
-- MAGIC comprehensive view of machine health with sensor details.

-- COMMAND ----------

SELECT
  m.machine_name,
  m.production_line,
  m.factory,
  s.sensor_name,
  s.sensor_type,
  s.unit,
  r.reading_time,
  r.value,
  r.status AS reading_status
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.sensor_readings r
  ON CAST(m.machine_id AS INT) = r.machine_id
JOIN glue_factory.lhf_demo_factory_master.sensors s
  ON r.sensor_id = CAST(s.sensor_id AS INT)
WHERE r.status != 'normal'
ORDER BY m.production_line, r.reading_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Anomaly Summary per Machine
-- MAGIC
-- MAGIC Aggregate warning and critical readings per machine across all sensor types.

-- COMMAND ----------

SELECT
  m.machine_name,
  m.production_line,
  m.factory,
  COUNT(*) AS total_readings,
  COUNT(CASE WHEN r.status = 'warning' THEN 1 END) AS warning_count,
  COUNT(CASE WHEN r.status = 'critical' THEN 1 END) AS critical_count,
  ROUND(
    COUNT(CASE WHEN r.status IN ('warning', 'critical') THEN 1 END) * 100.0 / COUNT(*),
    1
  ) AS anomaly_rate_pct
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.sensor_readings r
  ON CAST(m.machine_id AS INT) = r.machine_id
GROUP BY m.machine_name, m.production_line, m.factory
ORDER BY critical_count DESC, warning_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Machine Downtime Analysis
-- MAGIC
-- MAGIC Combine error/maintenance events with machine metadata to calculate
-- MAGIC total downtime per production line.

-- COMMAND ----------

SELECT
  m.production_line,
  m.factory,
  e.event_type,
  COUNT(*) AS event_count,
  SUM(e.duration_minutes) AS total_downtime_min,
  ROUND(SUM(e.duration_minutes) / 60.0, 1) AS total_downtime_hours
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.production_events e
  ON CAST(m.machine_id AS INT) = e.machine_id
WHERE e.event_type IN ('error', 'maintenance')
  AND e.duration_minutes IS NOT NULL
GROUP BY m.production_line, m.factory, e.event_type
ORDER BY total_downtime_min DESC;
