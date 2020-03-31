from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class HiveQueryReader(IStep):
	def __init__(self, query, table_name="hive_query"):
		self.query = query
		self.table_name = table_name

	def process(self, cardo_context, cardo_dataframe=None):
		return CardoDataFrame(cardo_context.spark.sql(self.query), self.table_name)
