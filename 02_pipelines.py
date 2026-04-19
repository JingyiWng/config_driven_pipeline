# ==============================================================
# 02_pipelines.py  —  YOUR TASK
#
# Implement 3 child pipelines that inherit from BasePipeline.
# Each one only needs to override transform().
#
# TransactionsPipeline:
#   - drop rows where amount is null
#   - keep only rows where amount > 0
#   - add a new column: amount_eur = round(amount * 0.92, 2)
#
# AccountsPipeline:
#   - drop rows where owner is null
#   - convert the owner column to uppercase
#
# FxRatesPipeline:
#   - drop rows where rate is null
#
# Run 01_base_pipeline.py first (or paste the class above this)
# ==============================================================

from pyspark.sql import functions as F


class TransactionsPipeline(BasePipeline):

    def transform(self, df):
        pass


class AccountsPipeline(BasePipeline):

    def transform(self, df):
        pass


class FxRatesPipeline(BasePipeline):

    def transform(self, df):
        pass
