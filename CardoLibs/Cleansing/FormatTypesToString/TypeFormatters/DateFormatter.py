from CardoLibs.Cleansing.FormatTypesToString.ITypeFormatter import ITypeFormatter
from pyspark.sql.functions import date_format
from pyspark.sql.types import TimestampType, DateType


class DateFormatter(ITypeFormatter):
	def __init__(self, date_format='YYYY-MM-dd HH:mm:ss'):
		self.date_format = date_format

	def is_type(self, spark_type):
		return type(spark_type) in [DateType, TimestampType]

	def convert(self, column):
		return date_format(column, self.date_format)