from CardoLibs.IO.JDBC.JDBCWriter.JDBCWriter import JDBCWriter


class MSSQLWriter(JDBCWriter):
    properties = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

    def __init__(self, table_name, connection_string, mode, properties=None, batchsize=50000):
        # type: (str, str, str, dict, int) -> None
        super(MSSQLWriter, self).__init__(table_name,
                                          connection_string,
                                          mode,
                                          dict(MSSQLWriter.properties, **properties or {}),
                                          batchsize)
