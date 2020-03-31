from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class HivePartitionWriter(IStep):
	def __init__(self, table_name, partition_column, partition_value=None, mode="overwrite"):
		self.table_name = table_name
		self.partition_column = partition_column
		self.partition_value = partition_value
		if mode not in ["overwrite", "append"]:
			raise ValueError("mode can be only overwrite, append")
		self.mode = mode

	def process(self, cardo_context, cardo_dataframe):
		# type: (CardoContext, CardoDataFrame) -> CardoDataFrame
		cardo_dataframe.dataframe.createOrReplaceTempView(cardo_dataframe.table_name)
		overwrite = self.mode if self.mode == "overwrite" else "into"
		if self.partition_value is not None:
			partition_value_string = "{}='{}'".format(self.partition_column, self.partition_value)
		else:
			partition_value_string = self.partition_column
		query = """
			insert {should_overwrite} table {table_name} partition ({partition_column})
			select * from {dataframe_name}
		""".format(should_overwrite=overwrite, table_name=self.table_name, partition_column=partition_value_string,
				dataframe_name=cardo_dataframe.table_name)
		cardo_context.logger.info("writing using query: {}".format(query))
		cardo_context.spark.sql(query)
