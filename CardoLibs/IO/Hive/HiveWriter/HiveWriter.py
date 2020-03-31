from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep

from pyspark.sql import Window
import pyspark.sql.functions as F

TMP_COL = '___tmp___'
ROW_NUM_COL = '___row_num___'


class HiveWriter(IStep):
    def __init__(
            self, table_name, file_format='parquet',
            mode='overwrite', partition_by=None, overwrite_partition=False, num_partitions=None,is_new_partitions=False,
            **kwargs):
        # type: (str, str, str) -> None
        self.table_name = table_name
        self.file_format = file_format
        self.mode = mode
        self.partition_by = partition_by
        self.kwargs = kwargs
        self.overwrite_partition = overwrite_partition
        self.num_partitions = num_partitions
        self.is_new_partitions = is_new_partitions

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
        try:
            if self.overwrite_partition:
                self._handle_partition_overwriting(cardo_context, cardo_dataframe)
            else:
                cardo_dataframe.dataframe.write.saveAsTable(self.table_name, mode=self.mode, format=self.file_format,
                                                            partitionBy=self.partition_by, **self.kwargs)
                cardo_context.logger.info(
                    "wrote table: {table_name} to Hive MetaStore. mode: {mode}, format: {format}".format(
                        table_name=self.table_name, mode=self.mode, format=self.file_format))
        except Exception as e:
            cardo_context.logger.error(
                'failed to write table {table_name} to Hive. {e_type}: {e_msg}'.format(
                    table_name=self.table_name, e_type=type(e), e_msg=str(e)))
            raise

        return cardo_dataframe

    def _handle_partition_overwriting(self, cardo_context, cardo_dataframe):
        self._validate_overwrite_partitions_params()
        cardo_context.spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.repartition(self.num_partitions)
        cardo_context.logger.info('set partitionOverwriteMode to dynamic')
        if self.is_new_partitions:
            self._write_one_row_per_partitions(cardo_context, cardo_dataframe)
        cardo_dataframe.dataframe.select(self._get_target_table_columns(cardo_context)).write.insertInto(self.table_name, True)

    def _get_target_table_columns(self, cardo_context):
        return cardo_context.spark.table(self.table_name).columns

    def _validate_overwrite_partitions_params(self):
        if self.mode != 'overwrite_partitions':
            raise Exception("In order to overwrite partitions you must set mode to 'overwrite_partitions'")
        if self.num_partitions is None or self.partition_by is None:
            raise Exception("You must specify num partitions and partitions columns in partitionOverwriteMode")

    def _write_one_row_per_partitions(self, cardo_context, cardo_dataframe):
        w = Window.partitionBy(self.partition_by).orderBy(TMP_COL)
        cardo_dataframe.persist()
        tmp_df = cardo_dataframe.dataframe.withColumn(TMP_COL, F.lit(1)).withColumn(ROW_NUM_COL, F.row_number().over(w))
        tmp_df = tmp_df.where('{} = 1'.format(ROW_NUM_COL)).drop(ROW_NUM_COL).drop(TMP_COL)
        tmp_df.write.mode('append').partitionBy(self.partition_by).saveAsTable(self.table_name)