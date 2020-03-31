# -*- coding: utf-8 -*-
from CardoExecutor.Contract.IStep import IStep
from CardoLibs.IO.Oracle.OracleParallelReader.OracleParallelReaderConfig import *
from CardoLibs.IO.Oracle.OracleReader.OracleReader import OracleReader
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from math import ceil
from multiprocessing.dummy import Pool
from datetime import datetime


class OracleParallelReader(IStep):
	def __init__(self, table_owner, table_name, connection_string, select_clause='*', where_clause='1=1',
	             num_parallel=1, fetchsize=50000, use_partitions=True, check_view=False):
		# type: (str, str, str, str, str, int, int, bool, bool) -> None
		self.table_owner = table_owner.upper()
		self.table_name = table_name.upper()
		self.connection_string = connection_string
		self.select_clause = select_clause
		self.where_clause = where_clause
		self.num_parallel = num_parallel
		self.fetchsize = fetchsize
		self.use_partitions = use_partitions
		self.check_view = check_view
	
	def process(self, cardo_context, cardo_dataframe=None):
		hash_column = ROWID
		if self.check_view and self._check_table_or_view(cardo_context):
			self.use_partitions = False
			hash_column = self._get_view_hash_column(cardo_context)
		query_frame = GET_ROWS_FOR_PARTITION_QUERY.format(table_owner=self.table_owner, table_name=self.table_name,
		                                                  where_clause=self.where_clause,
		                                                  select_clause=self.select_clause,
		                                                  table_divider_clause=TABLE_DIVIDER_CLAUSE.format(
			                                                  row_index=hash_column))
		partitions_name_num_rows_list = None
		if self.use_partitions:
			subpartitions_list = self._get_table_partitions(cardo_context, get_subpartitions=True)
			partitions_list = self._get_table_partitions(cardo_context, get_subpartitions=False)
			partitions_name_num_rows_list = subpartitions_list or partitions_list
			if subpartitions_list:
				query_frame = query_frame.format(use_partition=USE_SUBPARTITION)
			else:
				query_frame = query_frame.format(use_partition=USE_PARTITION if partitions_list else
				DONT_USE_PARTITION)
		else:
			query_frame = query_frame.format(use_partition=DONT_USE_PARTITION)
		if partitions_name_num_rows_list:
			readers = self._create_readers_by_partitions(partitions_name_num_rows_list, query_frame)
		else:
			readers = [OracleReader(query_frame.format(num_parallel=self.num_parallel, index=reader_index),
			                        self.connection_string, fetchsize=self.fetchsize) for reader_index in
			           range(self.num_parallel)]
		pool = Pool(min(len(readers) // 10, 100) + 1)
		prev_log_level = cardo_context.logger.level
		cardo_context.logger.setLevel('ERROR')
		dataframes = pool.map(lambda reader: reader.process(cardo_context).dataframe, readers)
		cardo_context.logger.setLevel(prev_log_level)
		df_united = OracleParallelReader.merge_reduce(lambda df1, df2: df1.union(df2), dataframes)
		cardo_context.logger.info(
			u'read data from Oracle from {table_owner}.{table_name} using OracleParallelReader successfully'.format(
				table_owner=self.table_owner, table_name=self.table_name))
		return CardoDataFrame(df_united, table_name=self.table_name)
	
	@staticmethod
	def merge_reduce(func, collection):
		if not collection:
			return None
		if len(collection) == 1:
			return collection[0]
		else:
			return func(OracleParallelReader.merge_reduce(func, collection[:len(collection) // 2]),
			            OracleParallelReader.merge_reduce(func, collection[len(collection) // 2:]))
	
	def _create_readers_by_partitions(self, partitions_name_num_rows_list, query_frame):
		"""
		For every partition in the list, calculate the number of readers for that partition, then create readers for
		all partitions accodringly
		:param partitions_name_num_rows_list: list of (partition_name, num_rows)
		:param query_frame: query template for the readers, with {partition},{num_parallel},{index} to fill
		:return: list of readers that collectively read the entire table
		"""
		total_rows_num = sum([float(partition_num_rows_pair[NUM_ROWS]) or 1.0 for partition_num_rows_pair in
		                      partitions_name_num_rows_list])
		partition_name_num_of_readers = [{PARTITION_NAME: partition_num_rows_pair[PARTITION_NAME],
		                                  NUM_READERS: self._calc_partition_readers_share(
			                                  float(partition_num_rows_pair[NUM_ROWS]), total_rows_num)} for
		                                 partition_num_rows_pair in partitions_name_num_rows_list]
		readers = [self._create_reader(query_frame, partition_num_of_readers_pair, reader_index) for
		           partition_num_of_readers_pair in partition_name_num_of_readers for reader_index in
		           range(partition_num_of_readers_pair[NUM_READERS])]
		return readers
	
	def _create_reader(self, query_frame, partition_num_of_readers_pair, reader_index):
		return OracleReader(query_frame.format(partition=partition_num_of_readers_pair[PARTITION_NAME],
		                                       num_parallel=partition_num_of_readers_pair[NUM_READERS],
		                                       index=reader_index), self.connection_string, fetchsize=self.fetchsize)
	
	def _calc_partition_readers_share(self, num_rows, total_rows_num):
		"""
		For a partition with num_rows and a table with total_rows_num rows, return the number of readers to use for
		this partition, according to self.num_parallel
		For partitions with 0 or null num_rows, we asign at least 1 reader
		:param num_rows: number or rows in the partition
		:param total_rows_num: number or rows in the whole table
		:return: number of readers to use to read this table
		"""
		return int(ceil((self.num_parallel * (num_rows or 1.0)) / total_rows_num))
	
	def _get_table_partitions(self, cardo_context, get_subpartitions):
		"""
		Gets the partitions or subpartitions of the requested table and their num_rows
		:param cardo_context: cardo spark context
		:param get_subpartitions: boolean, True to get the table's subpartitions, False for partitions
		:return: list of sub/partitions and their num_rows
		"""
		return OracleReader(
			GET_PARTITIONS_QUERY.format(sub=SUB if get_subpartitions else NO_SUB, table_owner=self.table_owner,
			                            table_name=self.table_name), self.connection_string).process(
			cardo_context).dataframe.fillna(0).collect()
	
	def _check_table_or_view(self, cardo_context):
		return OracleReader(GET_IS_TABLE_OR_VIEW.format(table_owner=self.table_owner, table_name=self.table_name),
		                    self.connection_string).process(cardo_context).dataframe.collect()
	
	def _get_view_hash_column(self, cardo_context):
		columns = OracleReader(GET_ALL_TABLE_COLUMNS.format(table_owner=self.table_owner, table_name=self.table_name),
		                       self.connection_string).process(cardo_context).dataframe.collect()
		columns_names = [column[COLUMN_NAME] for column in columns]
		all_columns_concat_delimiter = '|| \'__@__\' ||'.join(columns_names)
		return all_columns_concat_delimiter
