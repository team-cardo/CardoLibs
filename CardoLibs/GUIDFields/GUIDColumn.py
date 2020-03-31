from CardoExecutor.Contract.IStep import IStep
from pyspark.sql.functions import sha1, concat_ws, lit, coalesce, col


class GUIDColumns(IStep):
    def __init__(self, output_column_name, include_cols=None, exclude_cols=None, custom_hash_separator="_"):
        # type: (str,[str],[str],str)
        self.include_cols = include_cols
        self.exclude_cols = exclude_cols
        self.output_column_name = output_column_name
        self.hash_separator = custom_hash_separator

    def __define_columns_to_hash(self, dataframe_columns):
        # type: ([str]) -> [str]
        if self.include_cols:
            columns = self.include_cols
        else:
            columns = dataframe_columns
        if self.exclude_cols:
            self.exclude_cols = [column.upper() for column in self.exclude_cols]
            columns = [col for col in columns if col.upper() not in self.exclude_cols]
        return [col.upper() for col in columns]

    def process(self, cardo_context, cardo_dataframe):
        columns = self.__define_columns_to_hash(cardo_dataframe.dataframe.columns)
        df = cardo_dataframe.dataframe
        columns.sort()
        cardo_context.logger.info(
            ' Adding GUID-column `{output_column_name}` based on this columns: `{columns_to_hash}`'.format(
                output_column_name=self.output_column_name, columns_to_hash=columns))
        df = df.withColumn(self.output_column_name, sha1(
            concat_ws(self.hash_separator, *[coalesce(col(c).cast("string"), lit(" ")) for c in columns])))
        cardo_context.logger.info(
            ' `{output_column_name}` Hash-column was added based on this columns: `{columns_to_hash}`'.format(
                output_column_name=self.output_column_name, columns_to_hash=columns))
        cardo_dataframe.dataframe = df
        return cardo_dataframe
