import re
import xlrd
from pyspark.sql.functions import regexp_replace
from six import string_types

def clean_col_type(type_):
    type_names = ["decimal", "timestamp", "string", "int", "double"]
    for type_name in type_names:
        if type_name in type_.lower():
            type_ = type_name
    return type_


def extract_type_from_col_name(col_name):
    # type: (str) -> str
    """col name look like <col_name: col_type>. This function returns only <col_type> in lowercase"""
    return col_name.replace(" ", "").split(':')[-1].lower()


def clean_invisible_characters(dataframe):
    invisible_ascii = r'[\x00-\x1F]|\u200B|\u200b'
    for col_name, col_type in dataframe.dtypes:
        if col_type == "string":
            dataframe = dataframe.withColumn(col_name, regexp_replace(col_name, invisible_ascii, ''))
    return dataframe


def clean_invisible(text):
    # type: (str) -> str
    if isinstance(text, string_types):
        return re.sub(u'\u200b', '', text)
    return text


def _filter_invisible(text):
    return text != u'\u200b'


def filter_invisible(values):
    return filter(_filter_invisible, values)


def find_sheet(cardo_context, book, sheet_name):
    for sheet in book.sheets():
        if sheet.name == sheet_name:
            return sheet
    cardo_context.logger.error("Could not find sheet {sheet_name}".format(sheet_name=sheet_name))


def read_file(cardo_context, filename):
    # type: (CardoContext) -> xlrd.Book
    try:
        return xlrd.open_workbook(filename)
    except xlrd.XLRDError:
        cardo_context.logger.error("Unsupported format, or corrupt file. Make sure file is of type .xlsx")
        raise
