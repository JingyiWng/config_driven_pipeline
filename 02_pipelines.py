# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 — Child Pipelines
# MAGIC **Your task:** implement 3 child pipelines that inherit from `BasePipeline`.
# MAGIC Each one only needs to override `transform()`.
# MAGIC
# MAGIC **TransactionsPipeline:**
# MAGIC - drop rows where `amount` is null
# MAGIC - keep only rows where `amount > 0`
# MAGIC - add a new column: `amount_eur = round(amount * 0.92, 2)`
# MAGIC
# MAGIC **AccountsPipeline:**
# MAGIC - drop rows where `owner` is null
# MAGIC - convert the `owner` column to uppercase
# MAGIC
# MAGIC **FxRatesPipeline:**
# MAGIC - drop rows where `rate` is null
# MAGIC
# MAGIC Run `01_base_pipeline` first (or paste the class above this cell).

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./01_base_pipeline

# COMMAND ----------

class TransactionsPipeline(BasePipeline):

    def transform(self, df):
        df_amt = df.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
        df_amt = df_amt.withColumn("amount_eur", F.round(F.col("amount") * 0.92, 2))
        return df_amt

# COMMAND ----------

class AccountsPipeline(BasePipeline):

    def transform(self, df):
        df_owner = df.filter(F.col("owner").isNotNull())
        df_owner = df_owner.withColumn("owner", F.upper(F.col("owner")))
        return df_owner
                                    

# COMMAND ----------

class FxRatesPipeline(BasePipeline):

    def transform(self, df):
        df_rate = df.filter(F.col("rate").isNotNull())
        return df_rate