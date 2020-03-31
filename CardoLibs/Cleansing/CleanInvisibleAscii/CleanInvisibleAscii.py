from CardoExecutor.Common.CardoColumn import CardoColumn
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from pyspark.sql.functions import regexp_replace


class CleanInvisibleAscii(IStep):
	def __init__(self, columns):
		# type: ([CardoColumn]) -> object
		self.columns = columns
		self.__invisible_ascii_regex = r'[\x00-\x1F]'

	def process(self, cardo_context, cardo_dataframe):
		# type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
		for column in self.columns:
			cardo_dataframe.dataframe = cardo_dataframe.dataframe.withColumn(column.output_column, regexp_replace(column.input_column,
																												  self.__invisible_ascii_regex,
																												  ''))
		return cardo_dataframe
