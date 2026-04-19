# ==============================================================
# 01_base_pipeline.py  —  YOUR TASK
#
# Implement the BasePipeline class.
# It should:
#   - accept name, source, target in __init__
#   - read a Spark table in extract()
#   - pass the DataFrame through unchanged in transform()
#   - write the DataFrame to a Spark table in load()
#   - orchestrate all three steps in run(), with print statements
# ==============================================================

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
