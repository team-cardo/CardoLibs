from CardoLibs.Cleansing.FormatTypesToString.ITypeFormatter import ITypeFormatter


class DefaultFormatter(ITypeFormatter):
	def is_type(self, spark_type):
		return True

	def convert(self, column):
		return column.cast("string")