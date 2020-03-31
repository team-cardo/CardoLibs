GET_QUEUE_CONFIG_QUERY_TEMPLATE = """
select *
from your_pools_maintenance.QSM_CONFIG
where project_name = '{project_name}'
"""

ADD_TO_QUEUE_QUERY_TEMPLATE = """
begin pkg_queue_stg_mgr.register_stg_to_queue(
'{project_name}',
'{table_owner}', 
'{stage_table_owner}',
'{target_table_name}',
'{stage_table_name}',
'{delete_stage_table}', 
'{sql_command}',
'{queue_status}',
'{comments}');
end;
"""

MAX_TABLE_NAME_LENGTH = 30

APPEND_TO_ORACLE = 'append'
OVERWRITE_TO_ORACLE = 'overwrite'

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"

DEFAULT_ROWS_PER_SPLIT = 1000000
