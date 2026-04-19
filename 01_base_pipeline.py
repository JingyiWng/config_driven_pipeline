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

class BasePipeline:

    def __init__(self, name: str, source: str, target: str):
        pass

    def extract(self):
        pass

    def transform(self, df):
        pass

    def load(self, df):
        pass

    def run(self):
        pass
