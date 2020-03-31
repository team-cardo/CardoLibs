from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class HdfsReader(IStep):
	def __init__(self, path, format, schema=None, options=None):
		self.path = path
		self.format = format
		self.schema = schema
		self.options = options or {}

	def process(self, cardo_context, cardo_dataframe=None):
		# type: (CardoContext) -> CardoDataFrame
		cardo_context.spark.catalog.refreshByPath(self.path)
		data = CardoDataFrame(cardo_context.spark.read.load(self.path, self.format, self.schema, **self.options))
		cardo_context.logger.info(
			u'read data from Hdfs from {path} successfully'.format(path=self.path))
		return data