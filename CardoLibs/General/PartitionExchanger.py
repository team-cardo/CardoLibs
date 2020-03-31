from CardoExecutor.Contract.IStep import IStep
from CardoLibs.IO.JDBC.JDBCQuerier.JDBCQuerier import JDBCQuerier
from CardoLibs.IO.Oracle.OracleWriter.OracleWriter import OracleWriter

class PartitionExchanger(IStep):
    def __init__(self, final_table_name, tmp_table_name, partition_name, connection_string, drop_temp=False,
                 validate_data=False, update_global_indexes=False, extra_queries=None):
        # type: (str, str, str, str, bool,bool,bool,list) -> None
        self.connection_string = connection_string
        self.final_table_name = final_table_name
        self.tmp_table_name = tmp_table_name
        self.partition_name = partition_name
        self.extra_queries = extra_queries
        self.drop_temp = drop_temp
        self.validate_data = validate_data
        self.update_global_indexes = update_global_indexes

    def process(self, cardo_context, cardo_dataframe):
        with JDBCQuerier(cardo_context, self.connection_string, OracleWriter.properties["driver"]) as querier:
            exchange_query = "alter table {table_name} exchange partition {partition_name} with table {tmp_table}".format(
                table_name=self.final_table_name, partition_name=self.partition_name, tmp_table=self.tmp_table_name)
            if self.extra_queries is not None:
                self.__execute_additional_queries(cardo_context, querier, self.extra_queries)
                exchange_query = "{} including indexes".format(exchange_query)
            if not self.validate_data:
                exchange_query = "{} without validation".format(exchange_query)
            if self.update_global_indexes:
                exchange_query = "{} update global indexes".format(exchange_query)
            cardo_context.logger.info(
                "exchange partition {} from {} to {}".format(self.partition_name, self.tmp_table_name,
                                                             self.final_table_name))
            querier.execute(exchange_query)
            if self.drop_temp:
                cardo_context.logger.info("dropping table {}".format(self.tmp_table_name))
                querier.execute("drop table {}".format(self.tmp_table_name))
        return cardo_dataframe

    def __execute_additional_queries(self, cardo_context, querier, queries):
        for query in queries:
            cardo_context.logger.debug("executing extra query {}".format(query))
            querier.execute(query)
