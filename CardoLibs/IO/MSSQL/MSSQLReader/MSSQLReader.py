from CardoLibs.IO.JDBC.JDBCReader.JDBCReader import JDBCReader


class MSSQLReader(JDBCReader):
    properties = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

    def __init__(self, table_name, connection_string, properties=None, parallel_col=None, lower_bound=None,
                 upper_bound=None, num_parallel=None, fetchsize=50000):
        # type: (str, str, dict, str, str, str ,int ,int) -> None
        super(MSSQLReader, self).__init__(table_name,
                                          connection_string,
                                          dict(MSSQLReader.properties, **properties or {}),
                                          parallel_col, lower_bound, upper_bound, num_parallel, fetchsize)
