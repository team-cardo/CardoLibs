TABLE_DIVIDER_CLAUSE = '''mod(ora_hash({row_index}), {{{{num_parallel}}}}) = {{{{index}}}}'''
ROWID = 'rowid'
ROWNUM = 'rownum'
SUB = 'sub'
NO_SUB = ''
PARTITION_NAME = 'PARTITION_NAME'
NUM_ROWS = 'NUM_ROWS'
NUM_READERS = 'num_readers'
GET_PARTITIONS_QUERY = '''(
select
	{sub}partition_name as partition_name, num_rows
from
	dba_tab_{sub}partitions
where
	table_owner = '{table_owner}' and
	table_name = '{table_name}'
)'''
GET_ROWS_FOR_PARTITION_QUERY = '''(
select
	{select_clause}
from
	{table_owner}.{table_name} {{use_partition}}
where
	{table_divider_clause}
and
	({where_clause})
)'''
GET_IS_TABLE_OR_VIEW = '''(
select *
from
    all_views
where
    owner = '{table_owner}'
and
    view_name = '{table_name}'
)'''
GET_ALL_TABLE_COLUMNS = '''(
select
	column_name
from
	dba_tab_columns
where
	owner = '{table_owner}'
and
    table_name = '{table_name}'
)'''
DONT_USE_PARTITION = ''
USE_PARTITION = 'partition({partition})'
USE_SUBPARTITION = 'subpartition({partition})'
COLUMN_NAME = 'COLUMN_NAME'
