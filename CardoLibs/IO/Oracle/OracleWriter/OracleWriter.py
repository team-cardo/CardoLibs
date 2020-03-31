from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase

from CardoLibs.IO.JDBC.JDBCWriter.JDBCWriter import JDBCWriter


class OracleWriter(JDBCWriter):
    properties = {"driver": "oracle.jdbc.OracleDriver"}

    def __init__(self, table_name, connection_string, mode, properties=None, truncate=False, batchsize=10):
        # type: (str, str, str, dict, bool ,int) -> None
        super(OracleWriter, self).__init__(table_name,
                                           connection_string,
                                           mode,
                                           dict(dict(OracleWriter.properties, **properties or {}),
                                                truncate=str(truncate).lower()),
                                           batchsize)

    @staticmethod
    def __convert_columns_to_upper_case(cardo_dataframe):
        # type: (CardoDataFrame) -> None
        columns_as_upper = map(lambda column: cardo_dataframe.dataframe[column].alias(column.upper()),
                               cardo_dataframe.dataframe.columns)
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.select(*columns_as_upper)

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
        self.__convert_columns_to_upper_case(cardo_dataframe)
        super(OracleWriter, self).process(cardo_context, cardo_dataframe)
        return cardo_dataframe
