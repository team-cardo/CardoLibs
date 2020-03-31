from CardoLibs.Cleansing.FormatTypesToString.ITypeFormatter import ITypeFormatter
from pyspark.sql.functions import udf
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType


class NumericalFormatter(ITypeFormatter):
	def __init__(self):
		self.__convert_udf = udf(NumericalFormatter.__convert)

	def is_type(self, spark_type):
		return type(spark_type) in [DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType]

	@staticmethod
	def __convert(value):
		if value is not None and float(value) is not None:
			if float(value).is_integer():
				return str(int(value))
			return str(float(value))

	def convert(self, column):
		return self.__convert_udf(column)
