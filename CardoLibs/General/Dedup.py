from CardoExecutor.Contract.IStep import IStep
import pyspark.sql.functions as f
from pyspark.sql import Window


class Dedup(IStep):
    def __init__(self, first_seen, last_seen, exclude_columns=(), include_columns=()):
        self.first_seen = first_seen
        self.last_seen = last_seen
        if exclude_columns and include_columns:
            raise ValueError("to avoid confusing behaviour, having both exclude and include columns is not allowed")
        self.blacklist_columns = exclude_columns
        self.whitelist_columns = include_columns

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
        df = cardo_dataframe.dataframe
        df = self._drop_duplicates(df)
        cardo_dataframe.dataframe = df
        return cardo_dataframe

    def cols(self, df):
        if self.whitelist_columns:
            return self.whitelist_columns
        return [col for col in df.columns if col not in self.blacklist_columns]

    def _drop_duplicates(self, df):
        cols = self.cols(df)
        w = Window.partitionBy(cols)
        df = df.withColumn(self.last_seen, f.max(self.last_seen).over(w))
        df = df.withColumn(self.first_seen, f.min(self.first_seen).over(w))
        df = df.dropDuplicates(cols)
        return df
