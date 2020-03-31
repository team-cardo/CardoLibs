from CardoLibs.IO.Oracle.OracleWriter.OracleWriter import OracleWriter


ORACLE_CONNECTION_STRING = 'your_connection_string'
TEMP_SCHEMA = 'your_SCHEMA'

ORACLE_DRIVER = OracleWriter.properties['driver']

ORACLE_CREATE_TEMP_TABLE_QUERY = 'CREATE TABLE {temp_table} AS (SELECT {columns} FROM {dest_table} WHERE 1=0)'

ORACLE_LOCK_DEST_TABLE = 'LOCK TABLE {dest_table} IN EXCLUSIVE MODE WAIT 1800'
ORACLE_TRUNCATE_TABLE = 'TRUNCATE TABLE {}'
ORACLE_DROP_TABLE = 'DROP TABLE {}'

MERGE_ORACLE_STATEMENT = """MERGE INTO
{dest_table} D
USING  (SELECT {temp_columns} FROM {temp_table} T LEFT JOIN {dest_table} D ON {on_query} WHERE ({time_constraint})) T
ON
(
{on_query}
)
WHEN matched
THEN UPDATE set
{update_query}
WHEN NOT matched
THEN insert
(
{dest_columns}
)
VALUES
(
{temp_columns}
)"""

DESTINATION_SHORTCUT = 'D'
TEMP_SHORTCUT = 'T'
MAX_TABLE_NAME_LENGTH = 30
