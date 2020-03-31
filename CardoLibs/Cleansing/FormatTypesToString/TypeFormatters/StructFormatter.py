from CardoLibs.Cleansing.FormatTypesToString.ITypeFormatter import ITypeFormatter
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType


class StructFormatter(ITypeFormatter):
	def __init__(self):
		self.__convert_udf = udf(StructFormatter.__convert)

	def is_type(self, spark_type):
		return type(spark_type) == StructType

	@staticmethod
	def __convert(value):
		if value is not None:
			value_dict = value.asDict()
			return str(value_dict)

	def convert(self, column):
		return self.__convert_udf(column)
