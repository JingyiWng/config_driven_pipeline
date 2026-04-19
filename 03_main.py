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

# Step 1 — registry
PIPELINE_REGISTRY = {
    # your code here
}

# COMMAND ----------

# Step 2 — read active pipelines from the config table
configs = None  # replace this

# COMMAND ----------

# Step 3 — loop and run
