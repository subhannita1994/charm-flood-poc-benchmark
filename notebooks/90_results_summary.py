# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Results summary (Gate 2)
# MAGIC Latency per test/variant (median of repeats) + cost per run. Cost = DBUs from `system.billing.usage` for the job run
# MAGIC × Azure list DBU rate (Jobs Compute and All-Purpose, A10) + VM pay-as-you-go $/h × run hours.

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
R = f"{C}.{S}.benchmark_results"

# A10 — Azure list prices (USD). DBU rates: Premium tier list (assumption; their sheet uses All-Purpose 0.551).
DBU_RATE = {"jobs": 0.30, "jobs_photon": 0.30, "all_purpose": 0.551, "serverless_jobs": 0.45}
# VM: Azure Retail Prices API, canadacentral, Linux pay-as-you-go "Virtual Machines" meters, fetched 2026-11-17.
VM_USD_H = {"Standard_E2ds_v6": 0.179, "Standard_E8ds_v6": 0.716, "Standard_E4as_v5": 0.248, "Standard_E8as_v5": 0.496,
            "Standard_E8ds_v5": 0.632, "Standard_E16ds_v5": 1.264, "Standard_E48ds_v5": 3.792}
VM_SPOT_USD_H = {"Standard_E2ds_v6": 0.0331, "Standard_E8ds_v6": 0.1323, "Standard_E4as_v5": 0.0467,
                 "Standard_E8as_v5": 0.0935, "Standard_E8ds_v5": 0.2172, "Standard_E16ds_v5": 0.4344, "Standard_E48ds_v5": 1.3033}
USD_TO_CAD = 1.4166  # their sheet

# COMMAND ----------

lat = spark.sql(f"""
  SELECT test_id, variant, stage, n_rows, measure, code_fidelity,
         get_json_object(compute, '$.shape') AS shape, get_json_object(compute, '$.type') AS compute_type,
         get_json_object(compute, '$.assets') AS assets,
         percentile(seconds, 0.5) FILTER (WHERE ok) AS median_s, count_if(ok) AS runs, count_if(NOT ok) AS failed_attempts
  FROM {R}
  GROUP BY ALL ORDER BY test_id, stage, n_rows, median_s""")
lat.createOrReplaceTempView("lat")
display(lat)

# COMMAND ----------

usage = spark.sql(f"""
  WITH runs AS (SELECT DISTINCT job_run_id, get_json_object(compute, '$.shape') shape FROM {R} WHERE job_run_id <> '')
  SELECT r.shape, r.job_run_id, u.sku_name, sum(u.usage_quantity) AS dbus
  FROM system.billing.usage u JOIN runs r ON u.usage_metadata.job_run_id = r.job_run_id
  GROUP BY ALL""")
display(usage)

# COMMAND ----------

# MAGIC %md ### Headline comparisons (filled in as the matrix completes)

# COMMAND ----------

display(spark.sql("""
  SELECT test_id, stage, n_rows, shape, assets, variant, measure, round(median_s, 2) median_s,
         round(n_rows / median_s, 1) rows_per_s
  FROM lat WHERE test_id IN ('T2a', 'T2b', 'T8') ORDER BY test_id, stage, n_rows, median_s"""))
