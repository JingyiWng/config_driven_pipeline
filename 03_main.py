# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 — Main Runner
# MAGIC **Your task:** wire everything together.
# MAGIC
# MAGIC 1. Build a `PIPELINE_REGISTRY` dict that maps each pipeline name (string) to its class
# MAGIC 2. Query `finance.pipeline_config` for all **active** pipelines
# MAGIC 3. Loop over the results — for each row:
# MAGIC    - look up the right class from the registry
# MAGIC    - if no class is found, print a warning and skip
# MAGIC    - otherwise instantiate it and call `run()`
# MAGIC
# MAGIC Run `01_base_pipeline` and `02_pipelines` first (or paste the classes above this cell).

# COMMAND ----------

# MAGIC %run ./01_base_pipeline
# MAGIC

# COMMAND ----------

# MAGIC %run ./02_pipelines

# COMMAND ----------

# Step 1 — registry
PIPELINE_REGISTRY = {
    # your code here
    "transactions":TransactionsPipeline,
    "accounts":AccountsPipeline,
    "fx_rates":FxRatesPipeline

}

# COMMAND ----------

# Step 2 — read active pipelines from the config table
configs = spark.table("finance.pipeline_config").filter(F.col("is_active") == True)

# COMMAND ----------

display(configs)

# COMMAND ----------

# from pyspark.sql.functions import collect_list, struct

# Collect configs as a list of Row objects
config_rows = configs.select("name", "source", "target").collect()

for row in config_rows:
    pipeline_class = PIPELINE_REGISTRY.get(row["name"])
    if not pipeline_class:
        print(f"Warning: No pipeline class found for '{row['name']}'. Skipping.")
        continue
    # print('Using pipeline class: ',pipeline_class)
    display(spark.createDataFrame([(str(pipeline_class),)], ["Using pipeline class"]))
    pipeline_instance = pipeline_class(row["name"], row["source"], row["target"])
    pipeline_instance.run()

# COMMAND ----------

config_rows

# COMMAND ----------

# Step 3 — loop and run

# COMMAND ----------

spark.table("finance.clean_transactions").display()


# COMMAND ----------

spark.table("finance.clean_accounts").display()


# COMMAND ----------

spark.table("finance.clean_fx_rates").display()
