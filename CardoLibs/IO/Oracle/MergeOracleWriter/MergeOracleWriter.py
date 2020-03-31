import uuid

from CardoExecutor.Contract.IStep import IStep

from CardoLibs.IO.JDBC.JDBCQuerier.JDBCQuerier import JDBCQuerier
from CardoLibs.IO.Oracle.MergeOracleWriter import MergeOracleWriterConfig as config
from CardoLibs.IO.Oracle.OracleWriter.OracleWriter import OracleWriter


class MergeOracleWriter(IStep):
    def __init__(self, destination_table, connection_string, oracle_merge_query=None, time_column_name=None,
                 pk_columns=None, sysdate_columns=None, temp_schema_name=config.TEMP_SCHEMA,
                 temp_table_connection_string=config.ORACLE_CONNECTION_STRING, temp_table_columns=None,
                 create_temp_table_query=None):
        # type:(str, str, str, str, [str], [str], str, str, [str], str) -> CardoDataFrame
        self.oracle_merge_query = oracle_merge_query
        self.full_destination_table = destination_table
        self.time_column_name = time_column_name
        self.connection_string = connection_string
        self.temp_schema_name = temp_schema_name
        self.full_temp_table = self.__create_temp_table_name()
        self.pk_columns = [pk_column.upper() for pk_column in pk_columns] if pk_columns is not None else None
        self.sysdate_columns = [] if sysdate_columns is None else [column.upper() for column in
                                                                   sysdate_columns]  # The convention for column names is uppercase only
        self.temp_table_connection_string = temp_table_connection_string
        self.temp_table_columns = temp_table_columns
        self.create_temp_table_query = create_temp_table_query
        if self.oracle_merge_query is not None and (
                            self.time_column_name is not None or self.pk_columns is not None or self.sysdate_columns != []):
            raise TypeError('Entered both default and custom parameters')

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContext, CardoDataFrame) -> CardoDataFrame
        self.__create_temp_table(cardo_context, cardo_dataframe)
        try:
            with JDBCQuerier(cardo_context, self.connection_string, config.ORACLE_DRIVER) as oracle_querier:
                cardo_dataframe.dataframe = self.__drop_null_columns(cardo_dataframe)
                self.__write_to_temp_table(cardo_context, cardo_dataframe)
                self.__merge_to_dest(cardo_context, cardo_dataframe, oracle_querier)
                self.__delete_temp_table(cardo_context)
            return cardo_dataframe
        except Exception as e:
            self.__delete_temp_table(cardo_context)
            cardo_context.logger.error('The run has failed...'.format(self.full_temp_table), extra={'error': e.message})
            raise

    def __create_temp_table(self, cardo_context, cardo_dataframe):
        # type: (CardoContext, CardoDataFrame) -> None
        try:
            with JDBCQuerier(cardo_context, self.temp_table_connection_string,
                             config.ORACLE_DRIVER) as temp_oracle_querier:
                cardo_context.logger.info("Creating temporary table named '{}'".format(self.full_temp_table))
                temp_table_columns = self.temp_table_columns if self.temp_table_columns is not None else cardo_dataframe.dataframe.columns
                temp_table_columns = ', '.join(temp_table_columns)
                if self.create_temp_table_query is None:
                    create_table_query = config.ORACLE_CREATE_TEMP_TABLE_QUERY.format(temp_table=self.full_temp_table,
                                                                                      dest_table=self.full_destination_table,
                                                                                      columns=temp_table_columns)
                else:
                    create_table_query = self.create_temp_table_query.format(temp_table=self.full_temp_table)
                temp_oracle_querier.cursor.execute(create_table_query)
        except Exception as e:
            cardo_context.logger.error('The run has failed...'.format(self.full_temp_table), extra={'error': e.message})
            raise

    def __write_to_temp_table(self, cardo_context, cardo_dataframe):
        # type: (CardoContext, CardoDataFrame) -> None
        writer = OracleWriter(self.full_temp_table, self.temp_table_connection_string, 'append')
        writer.process(cardo_context, cardo_dataframe)
        cardo_context.logger.info("Inserted data to temporary table")

    def __merge_to_dest(self, cardo_context, cardo_dataframe, oracle_querier):
        # type: (CardoContext, CardoDataFrame, JDBCQuerier) -> None
        self.__generate_query(cardo_context, cardo_dataframe)
        self.__lock_dest_table(oracle_querier)
        oracle_querier.cursor.execute(self.oracle_merge_query)
        self.__unlock_dest_table(oracle_querier)
        cardo_context.logger.info('Merged the temporary table into the main table')

    def __generate_query(self, cardo_context, cardo_dataframe):
        # type: (CardoContext, CardoDataFrame) -> None
        if self.oracle_merge_query is None:
            self.oracle_merge_query = self._create_merge_query(cardo_dataframe)
        else:
            self.oracle_merge_query = self.oracle_merge_query.format(source_table=self.full_temp_table)
        cardo_context.logger.info('Created the merge query'.format(), extra={'query': self.oracle_merge_query})
        cardo_context.logger.info('Created the merge query'.format(), extra={'query': self.oracle_merge_query})

    def _create_merge_query(self, cardo_dataframe):
        # type: (CardoDataFrame) -> str
        sql_on_query = self.__create_on_clause_query()
        columns_to_update = self.__create_update_columns_query(cardo_dataframe)
        dest_columns = self.__get_columns_query_by_table(cardo_dataframe, config.DESTINATION_SHORTCUT)
        temp_columns = self.__get_columns_query_by_table(cardo_dataframe, config.TEMP_SHORTCUT)
        time_constraint = (
            '(D.{time_col} < T.{time_col}) OR D.{time_col} IS NULL'.format(time_col=self.time_column_name.upper()))
        query_without_sysdate = config.MERGE_ORACLE_STATEMENT.format(dest_table=self.full_destination_table,
                                                                     temp_table=self.full_temp_table,
                                                                     on_query=sql_on_query,
                                                                     update_query=columns_to_update,
                                                                     dest_columns=dest_columns,
                                                                     temp_columns=temp_columns,
                                                                     time_constraint=time_constraint)
        for sysdate_column in self.sysdate_columns:
            query_without_sysdate = query_without_sysdate.replace(
                '{table}.{sysdate_column}'.format(table=config.TEMP_SHORTCUT, sysdate_column=sysdate_column), 'SYSDATE')
        return query_without_sysdate

    def __create_update_columns_query(self, cardo_dataframe):
        # type: (CardoDataFrame) -> str
        columns_to_update = ', '.join(
            ['D.{not_pk_column} = T.{not_pk_column}'.format(not_pk_column=column.upper()) for column in
             cardo_dataframe.dataframe.columns if column.upper() not in self.pk_columns])
        return columns_to_update

    def __create_on_clause_query(self):
        # type: () -> str
        identity_query = ['(D.{column} = T.{column})'.format(column=pk_column) for pk_column in self.pk_columns]
        return ' and '.join(identity_query)

    def __lock_dest_table(self, oracle_querier):
        # type: (JDBCQuerier) -> None
        oracle_querier.cursor.execute(config.ORACLE_LOCK_DEST_TABLE.format(dest_table=self.full_destination_table))

    def __create_temp_table_name(self):
        # type: () -> str
        unique_id_for_table = uuid.uuid4().hex[:10]
        temp_table_name = '{table_name}_{hash}'.format(table_name=self.full_destination_table.split('.')[-1],
                                                       hash=unique_id_for_table)
        if len(temp_table_name) > config.MAX_TABLE_NAME_LENGTH:
            temp_table_name = temp_table_name[len(temp_table_name) - config.MAX_TABLE_NAME_LENGTH:]
        if self.temp_schema_name != '':
            return '{schema_name}.{table_name}'.format(schema_name=self.temp_schema_name, table_name=temp_table_name)
        else:
            return temp_table_name

    def __delete_temp_table(self, cardo_context):
        # type: (CardoContext) -> None
        with JDBCQuerier(cardo_context, self.temp_table_connection_string, config.ORACLE_DRIVER) as temp_oracle_querier:
            temp_oracle_querier.cursor.execute(config.ORACLE_TRUNCATE_TABLE.format(self.full_temp_table))
            temp_oracle_querier.cursor.execute(config.ORACLE_DROP_TABLE.format(self.full_temp_table))
            cardo_context.logger.info('Deleted the temporary table')

    @staticmethod
    def __get_columns_query_by_table(cardo_dataframe, table_name):
        # type: (CardoDataFrame, str) -> str
        return ', '.join(['{table}.{column}'.format(table=table_name, column=column.upper()) for column in
                          cardo_dataframe.dataframe.columns])

    @staticmethod
    def __unlock_dest_table(oracle_querier):
        # type: (JDBCQuerier) -> None
        oracle_querier.cursor.execute('COMMIT')

    @staticmethod
    def __drop_null_columns(cardo_dataframe):
        # type: (CardoDataFrame) -> None
        df = cardo_dataframe.dataframe
        for column, type in df.dtypes:
            if type == 'null':  # Spark doesn't support columns with type VOID, so column is deleted and created later
                df = df.drop(column)
        return df
