from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class NoOpStep(IStep):
	def process(self, cardo_context):
		return CardoDataFrame(self.df, self.table_name)

	def __init__(self, df, table_name=''):
		self.df = df
		self.table_name = table_name