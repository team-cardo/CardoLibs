import os
import xlrd
import decimal
from dateutil import parser
from datetime import datetime
from pyspark.sql.utils import ParseException

from CardoExecutor.Common import CardoContext
from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame

from . import utils
from six import text_type

NO_BREAKING_SPACE = "\xc2\xa0"


class ExcelReader(IStep):
    def __init__(self, excel_filename, sheet_name="Sheet1", strptime_formats=None):
        self.filename = excel_filename
        self.sheet_name = sheet_name
        self.strptime_formats = strptime_formats or []
        decimal.setcontext(decimal.ExtendedContext)

    @staticmethod
    def find_in_path(path, full_path=False):
        # type: (str) -> [str]
        xl_format = ".xlsx"
        try:
            prefix = "%s/" % path if full_path else ""
            return [["{0}{file}".format(prefix, file=file_) for file_ in files[-1] if file_.endswith(xl_format)]
                    for files in os.walk(path)][0]
        except IndexError:
            return [""]

    def __parse_dates(self, value):
        # type: (unicode or float) -> datetime or unicode
        """
        handles 3 use cases - if excel converts the date into excel's internal format, uses xlrd's function to parse it.
          if it is a common date format, use dateutil.parse to parse it
          if it is an uncommon date format, can receive strptime to parse with
        """
        try:
            date_tuple = xlrd.xldate_as_tuple(value, datemode=0)
            return datetime(*date_tuple)
        except (ValueError, xlrd.XLDateError):
            pass
        try:
            return parser.parse(value)
        except ValueError:
            pass
        for strptime_format in self.strptime_formats:
            try:
                return datetime.strptime(value, strptime_format)
            except ValueError:
                pass
        return value

    def __clean_date(self, value):
        if value is u'':
            return None
        return self.__parse_dates(value)

    def __get_castable_types_dict(self):
        return {"decimal": lambda value: None if value is u'' else decimal.Decimal(str(value)),
                "string": lambda value: None if value is u'' else value,
                "int": lambda value: None if value is u'' else int(value),
                "double": lambda value: None if value is u'' else float(value),
                "boolean": lambda value: None if value is u'' else bool(value),
                "timestamp": self.__clean_date}

    def __cast_column_type(self, col, type_):
        # type: ([], str) -> [object]
        type_dict = self.__get_castable_types_dict()
        type_ = utils.clean_col_type(type_)
        column_cells = list()
        for cell in col:
            column_cells.append(type_dict.get(type_, lambda value: value)(utils.clean_invisible(cell)))
        return column_cells

    def __table_from_sheet(self, sheet):
        # type: (xlrd.sheet) -> [[object]]
        column_names = utils.filter_invisible(sheet.row_values(0))
        column_types = map(utils.extract_type_from_col_name, column_names)
        table = [list() for _ in utils.filter_invisible(sheet.col_values(0)[1:])]
        for column_index, _ in enumerate(column_types):
            values = utils.filter_invisible(sheet.col_values(column_index)[1:])  # first values is name and is dropped
            column = self.__cast_column_type(values, column_types[column_index])
            for i, value in enumerate(column):
                table[i].append(value)
        return table

    def __create_dataframe(self, cardo_context, schema, table):
        try:
            dataframe = cardo_context.spark.createDataFrame(table, schema=schema)
            cardo_context.logger.info("Finished reading excel file {xl} into dataframe".format(xl=self.filename))
        except (ValueError, ParseException) as e:
            message = "Could not parse datatypes. " \
                      "Check columns' format and see documentation"
            e.message = message
            e.desc = message
            cardo_context.logger.error(text_type(e))
            raise e
        return dataframe

    def process(self, cardo_context, cardo_dataframe=None):
        # type: (CardoContext, None) -> CardoDataFrame
        sheet = utils.find_sheet(cardo_context, utils.read_file(cardo_context, self.filename), self.sheet_name)
        table = self.__table_from_sheet(sheet)
        headers = utils.filter_invisible(sheet.row_values(0))
        schema = str([str(utils.clean_invisible(cell)) for cell in headers]).replace("'", "")[1:-1]
        dataframe = self.__create_dataframe(cardo_context, schema, table)
        return CardoDataFrame(dataframe, sheet.name)
