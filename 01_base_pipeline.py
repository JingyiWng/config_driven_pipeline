# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 — Base Pipeline
# MAGIC **Your task:** implement the `BasePipeline` class below.
# MAGIC
# MAGIC It should:
# MAGIC - accept `name`, `source`, `target` in `__init__`
# MAGIC - read a Spark table in `extract()`
# MAGIC - pass the DataFrame through unchanged in `transform()`
# MAGIC - write the DataFrame to a Spark table in `load()`
# MAGIC - orchestrate all three steps in `run()`, with print statements

# COMMAND ----------

spark.sql("USE CATALOG jenn_config_driven_pipeline")

# COMMAND ----------

class BasePipeline:

    def __init__(self, name: str, source: str, target: str):
        self.name = name
        self.source = source
        self.target = target

    def extract(self):
        df = spark.table(self.source)
        return df

    def transform(self, df):
        df_transformed = df
        return df_transformed

    def load(self, df):
        df.write.mode("overwrite").format("delta").saveAsTable(self.target)
        

    def run(self):
        df = self.extract()
        df_transformed = self.transform(df)
        self.load(df_transformed)
