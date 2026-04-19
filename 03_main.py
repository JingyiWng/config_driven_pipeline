# ==============================================================
# 03_main.py  —  YOUR TASK
#
# Wire everything together:
#
#   1. Build a PIPELINE_REGISTRY dict that maps each pipeline
#      name (string) to its class
#
#   2. Query finance.pipeline_config for all ACTIVE pipelines
#
#   3. Loop over the results. For each row:
#        - look up the right class from the registry
#        - if no class is found, print a warning and skip
#        - otherwise instantiate it and call run()
#
# Run 01 and 02 first (or paste the classes above this)
# ==============================================================


# Step 1 — registry
PIPELINE_REGISTRY = {
    # your code here
}


# Step 2 — read active pipelines from the config table
configs = None  # replace this


# Step 3 — loop and run
