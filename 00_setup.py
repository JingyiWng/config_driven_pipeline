# Databricks notebook source
# MAGIC %md
# MAGIC ## 00 — Setup
# MAGIC Run this first. Creates the config table and fake source data.

# COMMAND ----------

spark.sql("USE CATALOG jenn_config_driven_pipeline")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS finance")

spark.sql("""
  CREATE TABLE IF NOT EXISTS finance.pipeline_config (
    id        INT,
    name      STRING,
    source    STRING,
    target    STRING,
    is_active BOOLEAN
  )
""")

spark.sql("TRUNCATE TABLE finance.pipeline_config")

spark.sql("""
  INSERT INTO finance.pipeline_config VALUES
  (1, 'transactions', 'finance.raw_transactions', 'finance.clean_transactions', true),
  (2, 'accounts',     'finance.raw_accounts',     'finance.clean_accounts',     true),
  (3, 'fx_rates',     'finance.raw_fx_rates',     'finance.clean_fx_rates',     true),
  (4, 'loans',        'finance.raw_loans',         'finance.clean_loans',        false)
""")

# COMMAND ----------

display(spark.table("finance.pipeline_config"))

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

txn_data = [
    Row(txn_id=1, account_id=101, amount=250.00, currency="USD", txn_date=date(2024,1,5)),
    Row(txn_id=2, account_id=102, amount=-40.50, currency="USD", txn_date=date(2024,1,6)),
    Row(txn_id=3, account_id=101, amount=None,   currency="USD", txn_date=date(2024,1,7)),
]
acct_data = [
    Row(account_id=101, owner="Alice", type="CHECKING", opened_date=date(2020,3,1)),
    Row(account_id=102, owner="Bob",   type="SAVINGS",  opened_date=date(2019,7,15)),
    Row(account_id=103, owner=None,    type="CHECKING", opened_date=date(2021,11,3)),
]
fx_data = [
    Row(fx_id=1, from_currency="USD", to_currency="EUR", rate=0.92, rate_date=date(2024,1,5)),
    Row(fx_id=2, from_currency="USD", to_currency="GBP", rate=0.79, rate_date=date(2024,1,5)),
    Row(fx_id=3, from_currency="USD", to_currency="EUR", rate=None,  rate_date=date(2024,1,6)),
]

# COMMAND ----------

spark.createDataFrame(txn_data).write.mode("overwrite").saveAsTable("finance.raw_transactions")
spark.createDataFrame(acct_data).write.mode("overwrite").saveAsTable("finance.raw_accounts")
spark.createDataFrame(fx_data).write.mode("overwrite").saveAsTable("finance.raw_fx_rates")

print("Setup complete.")

# COMMAND ----------

display(spark.table("finance.raw_transactions"))

# COMMAND ----------

display(spark.table("finance.raw_accounts"))

# COMMAND ----------

display(spark.table("finance.raw_fx_rates"))