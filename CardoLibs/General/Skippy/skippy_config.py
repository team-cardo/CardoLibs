SKIPPY_LOG_LINE = "SKIPPY {message_type}: tried to run project {project_name} with start_dt: {start_dt} and end_dt: {end_dt}"
SKIPPY_SERIAL_ERROR = "SERIAL ERROR"
SKIPPY_SERIAL_SUCCESS = "SERIAL SUCCESS"
SKIPPY_WARNING = "WARNING"
SKIPPY_SUCCESS = "SUCCESS"
WITH_INTERSECT = "\nbut intersecting dts were found: {intersected_dts}"
NO_INTERSECT = "\nand no intersecting dts were found!"
NO_PREVIOUS = "\nbut previous dt: {previous_dt} was not found!"
PREVIOUS_WITH_NO_INTERSECT = "\nno intersecting dts were found, and previous dt: {previous_dt} was found!"
SKIPPY_DT_FORMAT = '%Y%m%d%H%M'
SKIPPY_DT_HISTORY_TABLE = 'your_schema_name.SKIPPY_DT_HISTORY'
SKIPPY_ORACLE_READ_QUERY = '''(
select * from {history_table}
where PROJECT_NAME = '{project_name}'
and dt between '{start_dt}' and '{end_dt}'
)'''
SKIPPY_ORACLE_INSERT_QUERY = '''
INSERT ALL
	{rows_to_insert}
select * from dual;
'''
SKIPPY_ORACLE_INSERT_ROW = '''INTO {history_table} (PROJECT_NAME, DT) VALUES (''{project_name}'', ''{dt}'')'''
DT = 'dt'


class SkippySerialError(Exception):
	pass
