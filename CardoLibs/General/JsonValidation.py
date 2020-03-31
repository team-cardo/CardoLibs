from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep

import pyspark.sql.functions as F

TEMP_VIEW_NAME = '__json_validation_view__'

class JsonValidation(IStep):
    def __init__(self, schema, json_col, udf_name, udf_path, udf_query, is_valid_json_column, schema_column_name):
        self.schema = schema
        self.json_col = json_col
        self.udf_name = udf_name
        self.udf_path = udf_path
        self.udf_query = udf_query
        self.is_valid_json_column = is_valid_json_column
        self.schema_column_name = schema_column_name

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
        cardo_dataframe = self._validate_json(cardo_context, cardo_dataframe, self.schema)
        valid_condition = F.col(self.is_valid_json_column).isNull()
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.withColumn(self.is_valid_json_column, F.when(valid_condition, F.lit('true')))
        return cardo_dataframe

    def _validate_json(self, cardo_context, cardo_dataframe, schema):
        cardo_context.spark.udf.registerJavaFunction(self.udf_name, self.udf_path)
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.withColumn(self.schema_column_name, F.lit(schema).cast('string'))
        cardo_dataframe.dataframe.createOrReplaceTempView(TEMP_VIEW_NAME)
        cardo_dataframe.dataframe = cardo_context.spark.sql(
            self.udf_query.format(data_column_name=self.json_col, schema_column_name=self.schema_column_name,
                                             output_column_name=self.is_valid_json_column, table_name=TEMP_VIEW_NAME))
        return cardo_dataframe