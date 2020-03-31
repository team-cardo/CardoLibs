import pyspark.sql.functions as F
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from CardoLibs.Cleansing.FormatTypesToString.TypeFormatters.BinaryFormatter import BinaryFormatter
from CardoLibs.Cleansing.FormatTypesToString.TypeFormatters.DateFormatter import DateFormatter
from CardoLibs.Cleansing.FormatTypesToString.TypeFormatters.DefaultFormatter import DefaultFormatter
from CardoLibs.Cleansing.FormatTypesToString.TypeFormatters.MultipleValuesFormatter import MultipleValuesFormatter
from CardoLibs.Cleansing.FormatTypesToString.TypeFormatters.NumericalFormatter import NumericalFormatter
from CardoLibs.Cleansing.FormatTypesToString.TypeFormatters.StructFormatter import StructFormatter



class FormatTypesToString(IStep):
	def __init__(self):
		self.__formatters = [DateFormatter(), NumericalFormatter(), MultipleValuesFormatter(), StructFormatter(), BinaryFormatter(), DefaultFormatter()]

	def process(self, cardo_context, cardo_dataframe):
		# type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
		new_columns = []
		for column in cardo_dataframe.dataframe.schema:
			relevant_formatters = filter(lambda formatter: formatter.is_type(column.dataType), self.__formatters)
			new_column = relevant_formatters[0].convert(cardo_dataframe.dataframe[column.name])
			new_column = F.lower(new_column).alias(column.name)
			new_columns.append(new_column)
		cardo_dataframe.dataframe = cardo_dataframe.dataframe.select(new_columns)
		return cardo_dataframe
