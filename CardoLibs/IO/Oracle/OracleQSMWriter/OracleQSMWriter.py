import json
import uuid
import math

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from CardoLibs.IO.JDBC.JDBCQuerier.JDBCQuerier import JDBCQuerier
from CardoLibs.IO.Oracle.OracleQSMWriter.OracleQsmWriterConfig import ADD_TO_QUEUE_QUERY_TEMPLATE, \
    GET_QUEUE_CONFIG_QUERY_TEMPLATE, MAX_TABLE_NAME_LENGTH, ORACLE_DRIVER, APPEND_TO_ORACLE, DEFAULT_ROWS_PER_SPLIT
from CardoLibs.IO.Oracle.OracleReader.OracleReader import OracleReader
from CardoLibs.IO.Oracle.OracleWriter.OracleWriter import OracleWriter


class OracleQsmWriter(IStep):
    def __init__(self, project_name, connection_string, main_query,
                 creation_query_template_stage_table, qsm_connection_string=None, delete_stage_after_command=True,
                 initial_qsm_status="pending", comments=None, rows_per_split=DEFAULT_ROWS_PER_SPLIT,
                 temp_table_hash=None, send_stage_to_qsm=True, create_temp_table=True, batchsize=10,
                 split_dataframe=True):
        self.batchsize = batchsize
        self.create_temp_table = create_temp_table
        self.send_stage_to_qsm = send_stage_to_qsm
        self.comments = comments
        self.initial_qsm_status = initial_qsm_status
        self.delete_stage_after_command = delete_stage_after_command
        self.creation_query_template_stage_table = creation_query_template_stage_table
        self.sql_command_template = main_query
        self.connection_string = connection_string
        self.qsm_connection_string = qsm_connection_string or connection_string
        self.project_name = project_name
        self.rows_per_split = rows_per_split
        self.temp_table_hash = temp_table_hash
        self.split_dataframe = split_dataframe

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContextBase, CardoDataFrame) -> CardoDataFrame
        original_cardo_dataframe = cardo_dataframe
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.persist()
        if self.split_dataframe:
            num_of_rows = cardo_dataframe.dataframe.count()
            if num_of_rows > 0:
                list_of_weights = range(int(math.ceil(num_of_rows / float(self.rows_per_split))))
                splited_dataframes = cardo_dataframe.dataframe.randomSplit([0.1 for _ in list_of_weights])
                for splited_dataframe in splited_dataframes:
                    self.send_df_to_qsm(cardo_context, splited_dataframe)
            elif self.send_stage_to_qsm and self.temp_table_hash:
                self.send_df_to_qsm(cardo_context, cardo_dataframe.dataframe)
        else:
            self.send_df_to_qsm(cardo_context, cardo_dataframe.dataframe)
        return original_cardo_dataframe

    def send_df_to_qsm(self, cardo_context, splited_dataframe):
        with JDBCQuerier(cardo_context, self.connection_string, ORACLE_DRIVER) as querier:
            qsm_queue_config = self._get_queue_config(cardo_context)
            stage_table_name = self.prepare_stage_table(cardo_context, CardoDataFrame(splited_dataframe),
                                                        qsm_queue_config, querier)
        if self.send_stage_to_qsm:
            with JDBCQuerier(cardo_context, self.qsm_connection_string, ORACLE_DRIVER) as qsm_querier:
                qsm_querier.execute(self._create_main_query(cardo_context, qsm_queue_config, stage_table_name))
                self._log_added_stage_table_to_qsm(cardo_context, stage_table_name)

    def _create_main_query(self, cardo_context, qsm_queue_config, stage_table_name):
        owner, table_name = self._get_owner_and_table_name(stage_table_name)
        qsm_sql_command = self.sql_command_template.format(stage_table_name=stage_table_name, owner=owner, table_name=table_name)
        comments = self._get_qsm_comment(cardo_context.run_id)
        stage_table_name_without_owner = stage_table_name.split('.')[1]
        self.delete_stage_after_command = "YES" if self.delete_stage_after_command else "NO"
        add_stage_table_to_queue_query = ADD_TO_QUEUE_QUERY_TEMPLATE.format(
            project_name=self.project_name,
            table_owner=qsm_queue_config["TABLE_OWNER"],
            stage_table_owner=qsm_queue_config["STG_TABLE_OWNER"],
            target_table_name=qsm_queue_config["TARGET_TABLE_NAME"],
            stage_table_name=stage_table_name_without_owner,
            delete_stage_table=self.delete_stage_after_command,
            sql_command=qsm_sql_command,
            queue_status=self.initial_qsm_status,
            comments=comments
        )
        self._log_sent_query_to_qsm(cardo_context, add_stage_table_to_queue_query)
        return add_stage_table_to_queue_query

    def prepare_stage_table(self, cardo_context, cardo_dataframe, qsm_queue_config, querier):
        creation_command_for_stage_table, stage_table_name = self.fill_create_stage_table_query(qsm_queue_config)
        if self.create_temp_table:
            querier.execute(creation_command_for_stage_table)
        self._log_creation_of_stage_table(cardo_context, stage_table_name, creation_command_for_stage_table)
        self._write_data_to_stage_table(cardo_dataframe, cardo_context, stage_table_name)
        self._log_wrote_data_to_stage(cardo_context, stage_table_name)
        return stage_table_name

    def fill_create_stage_table_query(self, qsm_queue_config):
        stage_table_name_prefix = qsm_queue_config["STG_TEMPLATE_NAME"].replace("#", "")
        stage_table_name = self._get_temp_table_name(stage_table_name_prefix, qsm_queue_config["STG_TABLE_OWNER"])
        stage_table_tablespace = qsm_queue_config["TABLESPACE_NAME"]
        creation_command_for_stage_table = self.creation_query_template_stage_table.format(table_name=stage_table_name,
                                                                                           tablespace=stage_table_tablespace)
        return creation_command_for_stage_table, stage_table_name

    def _get_queue_config(self, context):
        oracle_reader_query = "({query})".format(
            query=GET_QUEUE_CONFIG_QUERY_TEMPLATE.format(project_name=self.project_name))
        df = OracleReader(oracle_reader_query, self.qsm_connection_string).process(cardo_context=context).dataframe
        return df.collect()[0].asDict()

    def _write_data_to_stage_table(self, cardo_dataframe, cardo_context, stage_table_name):
        OracleWriter(stage_table_name, self.connection_string, APPEND_TO_ORACLE, batchsize=self.batchsize).process(cardo_context, cardo_dataframe)

    def _get_qsm_comment(self, run_id):
        if self.comments is None:
            comments = {"run_id": run_id}
        else:
            comments = self.comments
            comments["run_id"] = run_id
        return json.dumps(comments)

    def _log_creation_of_stage_table(self, cardo_context, stage_table_name, creation_command_for_stage_table):
        cardo_context.logger.info(
            'created stage table "{stage_table_name}" for project "{project}"'.format(
                stage_table_name=stage_table_name, project=self.project_name),
            extra={
                "query": creation_command_for_stage_table,
                "stage_table_name": stage_table_name,
                "carpool_project_name": self.project_name
            })

    def _log_wrote_data_to_stage(self, cardo_context, stage_table_name):
        cardo_context.logger.info(
            'wrote data to stage table {stage_table_name} successfully'.format(stage_table_name=stage_table_name),
            extra={
                "stage_table_name": stage_table_name,
                "carpool_project_name": self.project_name
            })

    def _log_added_stage_table_to_qsm(self, cardo_context, stage_table_name):
        cardo_context.logger.info(
            'added stage table "{stage_table_name}" to qsm "{project_name}" successfully'.format(
                stage_table_name=stage_table_name, project_name=self.project_name),
            extra={
                "stage_table_name": stage_table_name,
                "carpool_project_name": self.project_name
            })

    def _get_temp_table_name(self, stage_table_name_prefix, stage_schema_name):
        # type: () -> str
        unique_id_for_table = self.temp_table_hash or uuid.uuid4().hex[:10]
        stage_table_name = '{table_name}_{hash}'.format(table_name=stage_table_name_prefix, hash=unique_id_for_table)
        if len(stage_table_name) > MAX_TABLE_NAME_LENGTH:
            stage_table_name = stage_table_name[len(stage_table_name) - MAX_TABLE_NAME_LENGTH:]
        return '{schema_name}.{table_name}'.format(schema_name=stage_schema_name, table_name=stage_table_name)

    def _log_sent_query_to_qsm(self, cardo_context, queue_query_name):
        cardo_context.logger.info(
            'The query {queue_query_name} sent successfully'.format(queue_query_name=queue_query_name),
            extra={
                "queue_query_name": queue_query_name,
                "carpool_project_name": self.project_name
            })

    def _get_owner_and_table_name(self, full_table_name):
        splitted = full_table_name.split('.')
        owner, table_name = splitted[0], splitted[1]
        return owner, table_name
