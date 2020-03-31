from CardoLibs.Cleansing.FormatTypesToString.ITypeFormatter import ITypeFormatter
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType


class BinaryFormatter(ITypeFormatter):
	def __init__(self):
		self.__convert_udf = udf(BinaryFormatter.__convert)

	def is_type(self, spark_type):
		return type(spark_type) == BinaryType

	@staticmethod
	def __convert(value):
		if value is not None:
			return str(value).encode('hex')

	def convert(self, column):
		return self.__convert_udf(column)
