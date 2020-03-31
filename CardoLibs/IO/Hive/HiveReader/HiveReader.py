from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame


class HiveReader(IStep):
	def __init__(self, table_name, partitions=None):
		# type: (str, int) -> None
		self.table_name = table_name
		self.partitions = partitions

	def process(self, cardo_context, cardo_dataframe=None):
		# type: (CardoContextBase) -> CardoDataFrame
		df = cardo_context.spark.table(self.table_name)
		if self.partitions is not None:
			df = df.repartition(self.partitions)
		cardo_context.logger.info('finished to read table: {table_name} from Hive MetaStore'.format(table_name=self.table_name))
		return CardoDataFrame(df, self.table_name)
