from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame


class SqlReader(IStep):
	def __init__(self, query, table_name=None):
		# type: (str,str) -> None
		self.query = query
		self.table = table_name

	def process(self, cardo_context, cardo_dataframe=None):
		# type: (CardoContextBase) -> CardoDataFrame
		df = cardo_context.spark.sql(self.query)
		cardo_context.logger.info('finished to read query: {table_name} from Hive MetaStore'.format(table_name=self.query))
		return CardoDataFrame(df, self.table)
