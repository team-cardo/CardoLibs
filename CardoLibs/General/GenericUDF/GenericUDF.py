from collections import namedtuple

import pyspark.sql.functions as F
import requests
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep

CONFIG_INSULIN_ROUTE = "your url to insulin api"
CONFIG_FETCH_ROUTE = "{server}/udf_config".format(server=CONFIG_INSULIN_ROUTE)

CONFIG_OUTPUT_COLUMN = "temp_output"
CONFIG_TEMP_TABLE_HASH_LENGTH = 8

Config = namedtuple("Config", ["udf_name", "location", "parameters", "outputParameters", "defaultProp"])


class GenericUDF(IStep):
    def __init__(self, udf_name, columns, keep_columns=None, config=None, save_failed_values=False,
                 save_failed_configuration=None):
        # type: (str, dict, list, dict, bool, dict) -> None
        self.udf_name = udf_name
        self.columns = columns
        self.keep_columns = keep_columns
        self.config = Config(**(config if config else GenericUDF._fetch_udf_config(udf_name)))
        self.save_failed_values = save_failed_values
        self.save_failed_configuration = save_failed_configuration or {}
        self.fill_missing_columns()

    @staticmethod
    def _fetch_udf_config(udf_name):
        try:
            config = requests.get(CONFIG_FETCH_ROUTE, timeout=10).json()
            return filter(lambda x: x.get("udf_name") == udf_name, config)[0]
        except Exception:
            raise Exception("config server is not working from {server_name}".format(server_name=CONFIG_INSULIN_ROUTE))

    @staticmethod
    def _get_column_names(columns):
        return map(lambda col: col.split(":")[0], columns.split(","))

    def fill_missing_columns(self):
        columns_needed = GenericUDF._get_column_names(self.config.parameters)
        for column in columns_needed:
            if column not in self.columns.keys():
                self.columns[column] = "null"

    def extract_columns_and_drop(self, df):
        for cardo_column in self.keep_columns:
            df = df.withColumn(cardo_column.output_column, df[
                "{output}.{column}".format(output=CONFIG_OUTPUT_COLUMN, column=cardo_column.input_column)])
        if self.save_failed_values:
            df = self.handle_failed_values(df, self.save_failed_configuration.get("main_column"),
                                               self.save_failed_configuration.get("sub_column"))
        return df.drop(CONFIG_OUTPUT_COLUMN)

    def generate_udf_parameters(self):
        return "({params})".format(
            params=",".join([self.columns.get(c) for c in self._get_column_names(self.config.parameters)]))

    def handle_failed_values(self, df, main_udf_column, sub_udf_column):
        new_column_value = df[main_udf_column]
        new_column_value = F.coalesce(new_column_value, df[sub_udf_column])
        return df.withColumn(main_udf_column, new_column_value)

    def process(self, cardo_context, cardo_dataframe):
        cardo_context.spark.udf.registerJavaFunction(self.config.udf_name, self.config.location)
        df = cardo_dataframe.dataframe
        df = df.withColumn(CONFIG_OUTPUT_COLUMN, F.expr(
            "{udf_name}{udf_params}".format(udf_name=self.udf_name, udf_params=self.generate_udf_parameters())))
        df = self.extract_columns_and_drop(df)
        return CardoDataFrame(df)
