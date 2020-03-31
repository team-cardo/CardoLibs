# -*- coding: utf-8 -*-

from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame


class JDBCReader(IStep):
	def __init__(self, table_name, connection_string, properties, parallel_col=None, lower_bound=None, upper_bound=None, num_parallel=None, fetchsize=50000):
		# type: (str, str, dict, str, str, str ,int ,int) -> None
		self.table_name = table_name
		self.connection_string = connection_string
		self.properties = properties
		self.properties.update({"fetchsize": str(fetchsize)})
		self.parallel_col = parallel_col
		self.lower_bound = lower_bound
		self.upper_bound = upper_bound
		self.num_parallel = num_parallel

	def process(self, cardo_context, cardo_dataframe=None):
		# type: (CardoContext) -> CardoDataFrame
		df = cardo_context.spark.read.jdbc(
				self.connection_string,
				self.table_name,
				column=self.parallel_col,
				lowerBound=self.lower_bound,
				upperBound=self.upper_bound,
				numPartitions=self.num_parallel,
				properties=self.properties
		)
		cardo_context.logger.info(
				u'read data with {reader} from table {table_name} using connection string : {connection_string} properties: {properties}'.format(
					reader=self.__class__.__name__, table_name=self.table_name, connection_string=self.connection_string, properties=self.properties))
		return CardoDataFrame(dataframe=df, table_name=self.table_name)
