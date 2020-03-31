from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class HdfsWriter(IStep):
	def __init__(self, path, mode, format='parquet', partitionBy=None, options=None):
		self.path = path
		self.format = format
		self.mode = mode
		self.partitionBy = partitionBy
		self.options = options or {}

	def process(self, cardo_context, cardo_dataframe):
		# type: (CardoContext, CardoDataFrame) -> CardoDataFrame
		try:
			cardo_context.spark.catalog.refreshByPath(self.path)
			cardo_dataframe.dataframe.write.save(self.path, self.format, self.mode, self.partitionBy, **self.options)
			cardo_context.logger.info(u"wrote datfaframe to {}".format(self.path))
			return cardo_dataframe
		except Exception as e:
			cardo_context.logger.error(u"failed to write dataframe to {}. {}: {}".format(self.path, type(e), str(e)))