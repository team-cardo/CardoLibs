from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame


class JDBCWriter(IStep):
	def __init__(self, table_name, connection_string, mode, properties, batchsize=50000):
		# type: (str, str, str, dict, int) -> None
		self.table_name = table_name
		self.connection_string = connection_string
		self.mode = mode
		self.properties = properties
		self.properties.update({"batchsize": str(batchsize)})

	def process(self, cardo_context, cardo_dataframe):
		# type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
		cardo_dataframe.dataframe.write.jdbc(
				self.connection_string,
				self.table_name,
				self.mode,
				properties=self.properties
		)
		cardo_context.logger.info(
				'wrote dataframe data using {reader} to {table_name} using connection string: {connection_string} in mode: {mode} successfully'.format(
						reader=self.__class__.__name__, table_name=self.table_name, connection_string=self.connection_string, mode=self.mode))
		return cardo_dataframe
