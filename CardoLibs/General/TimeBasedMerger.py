from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep

import pyspark.sql.functions as F
from pyspark.sql import Window

VALID_COL = '___valid___'
DIFF_COL = '___diff___'
SUBGROUP_COL = '___subgroup___'
INT_TYPE = 'int'
TIMESTAMP_TYPE = 'timestamp'

class TimeBasedMerger(IStep):
    def __init__(self, time_column, group_by_columns, aggs, max_merge_time=16*60, first_time_column='FIRST_TIME',
                 last_time_column='LAST_TIME', minimal_stay_time=0, time_format=None):
        self.time_column = time_column
        self.group_by_columns = group_by_columns
        self.minimal_stay_time = minimal_stay_time
        self.max_merge_time = max_merge_time
        self.aggregations = aggs
        self.first_time_column = first_time_column
        self.last_time_column = last_time_column
        self.merge_window = Window.partitionBy(self.group_by_columns).orderBy(self.time_column)
        self.time_format = time_format


    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> (CardoDataFrame)
        df = cardo_dataframe.dataframe
        df = self._filter_nulls(df, self.group_by_columns + [self.time_column])
        df = self._convert_to_timestamp(df, self.time_column)
        df = self._add_time_diff(df)
        df = self._replace_nulls_with_zero(df, DIFF_COL)
        df = self._merge_rows(df, self.max_merge_time)
        df = self._filter_minimum_stay_time(df, self.minimal_stay_time)
        cardo_dataframe.dataframe = df
        return cardo_dataframe

    def _merge_rows(self, df, samples_max_merge_time):
        df = df.withColumn(VALID_COL, (df[DIFF_COL] >= samples_max_merge_time).cast(INT_TYPE))
        df = df.withColumn(SUBGROUP_COL, F.sum(VALID_COL).over(self.merge_window) - F.col(VALID_COL))
        df = df.groupBy(*(self.group_by_columns + [SUBGROUP_COL])).agg(*self.aggregations)
        df = df.drop(SUBGROUP_COL)
        return df

    def _add_time_diff(self, df):
        dedup_cols = self.group_by_columns + [self.time_column]
        df = df.dropDuplicates(dedup_cols)
        df = df.withColumn(DIFF_COL, F.abs(
            F.unix_timestamp(F.lead(self.time_column).over(self.merge_window)) - F.unix_timestamp(F.col(self.time_column))))
        return df

    def _filter_minimum_stay_time(self, df, minimal_stay_time):
        return df.where(
            (F.unix_timestamp(F.col(self.last_time_column)) - F.unix_timestamp(
                F.col(self.first_time_column))) >= minimal_stay_time)

    def _convert_to_timestamp(self, df, *columns):
        #Keep in mind that to_timestamp convert the time to the timezone of the system unless you set in the config otherwise (see spark.sql.session.timeZone)
        for columns in columns:
            df = df.withColumn(columns, F.to_timestamp(columns, self.time_format))
        return df

    def _filter_nulls(self, df, columns):
        for column in columns:
            df = df.where(F.col(column).isNotNull())
        return df

    def _replace_nulls_with_zero(self, df, *columns):
        for column in columns:
            df = df.withColumn(column, F.when(F.col(column).isNull(), 0).otherwise(F.col(column)))
        return df
