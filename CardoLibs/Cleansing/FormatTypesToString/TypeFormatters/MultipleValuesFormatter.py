from CardoLibs.Cleansing.FormatTypesToString.ITypeFormatter import ITypeFormatter
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, MapType


class MultipleValuesFormatter(ITypeFormatter):
	def __init__(self):
		self.__convert_udf = udf(MultipleValuesFormatter.__convert)

	def is_type(self, spark_type):
		return type(spark_type) in [ArrayType, MapType]

	@staticmethod
	def __convert(value):
		if value is not None:
			return str(value)

	def convert(self, column):
		return self.__convert_udf(column)
