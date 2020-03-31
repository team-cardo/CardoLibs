from pyspark.sql import Column
from pyspark.sql.types import DataType


class ITypeFormatter:
	def is_type(self, spark_type):
		# type: (DataType) -> bool
		raise NotImplementedError()

	def convert(self, column):
		# type: (Column) -> Column
		raise NotImplementedError()