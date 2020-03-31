from cardo.core import CardoContext
from cardo.io import OracleReader
from datetime import datetime, timedelta

from skippy_config import *


class Skippy(object):
	def __init__(self, cardo_context, oracle_connection_string, project_name, start_datetime, end_datetime,
	             period=timedelta(hours=1), is_serial=False, skippy_dt_history_table=SKIPPY_DT_HISTORY_TABLE):
		# type: (CardoContext, str, str, datetime, datetime, timedelta, bool, str) -> None
		self.cardo_context = cardo_context
		self.oracle_connection_string = oracle_connection_string
		self.project_name = project_name
		self.start_datetime = start_datetime
		self.end_datetime = end_datetime
		self.period = period
		self.is_serial = is_serial
		self.skippy_dt_history_table = skippy_dt_history_table
	
	def check_current_dt(self):
		start_dt = self.start_datetime.strftime(SKIPPY_DT_FORMAT)
		end_dt = self.end_datetime.strftime(SKIPPY_DT_FORMAT)
		previous_dt = self._get_previous_dt()
		current_dt_list = self._create_current_dts_list()
		history_dt_list = self._get_dt_history(previous_dt, end_dt)
		dts_intersection = set(current_dt_list) & set(history_dt_list)
		return self._get_result(dts_intersection, end_dt, history_dt_list, previous_dt, start_dt)
	
	def insert_current_dt(self):
		return SKIPPY_ORACLE_INSERT_QUERY.format(rows_to_insert='\n\t'.join([
			SKIPPY_ORACLE_INSERT_ROW.format(history_table=self.skippy_dt_history_table, project_name=self.project_name,
			                                dt=dt) for dt in self._create_current_dts_list()]))
	
	def _get_result(self, dts_intersection, end_dt, history_dt_list, previous_dt, start_dt):
		if not dts_intersection:
			if self.is_serial:
				if previous_dt in history_dt_list:
					return self._serial_success(end_dt, previous_dt, start_dt)
				else:
					raise self._serial_error(end_dt, previous_dt, start_dt)
			else:
				return self._skippy_success(end_dt, previous_dt, start_dt)
		else:
			return self._skippy_warning(dts_intersection, end_dt, previous_dt, start_dt)
	
	def _skippy_warning(self, dts_intersection, end_dt, previous_dt, start_dt):
		self.cardo_context.logger.warn(
			(SKIPPY_LOG_LINE + WITH_INTERSECT).format(message_type=SKIPPY_WARNING, project_name=self.project_name,
			                                          start_dt=start_dt, end_dt=end_dt, previous_dt=previous_dt,
			                                          intersected_dts=', '.join(dts_intersection)))
		return False
	
	def _skippy_success(self, end_dt, previous_dt, start_dt):
		self.cardo_context.logger.info(
			(SKIPPY_LOG_LINE + NO_INTERSECT).format(message_type=SKIPPY_SUCCESS, project_name=self.project_name,
			                                        start_dt=start_dt, end_dt=end_dt, previous_dt=previous_dt))
		return True
	
	def _serial_error(self, end_dt, previous_dt, start_dt):
		error_message = (SKIPPY_LOG_LINE + NO_PREVIOUS).format(message_type=SKIPPY_SERIAL_ERROR,
		                                                       project_name=self.project_name, start_dt=start_dt,
		                                                       end_dt=end_dt, previous_dt=previous_dt)
		self.cardo_context.logger.error(error_message)
		return SkippySerialError(error_message)
	
	def _serial_success(self, end_dt, previous_dt, start_dt):
		self.cardo_context.logger.info(
			(SKIPPY_LOG_LINE + PREVIOUS_WITH_NO_INTERSECT).format(message_type=SKIPPY_SERIAL_SUCCESS,
			                                                      project_name=self.project_name, start_dt=start_dt,
			                                                      end_dt=end_dt, previous_dt=previous_dt))
		return True
	
	def _get_dt_history(self, start_dt, end_dt):
		oracle_reader = OracleReader(
			SKIPPY_ORACLE_READ_QUERY.format(history_table=self.skippy_dt_history_table, project_name=self.project_name,
			                                start_dt=start_dt, end_dt=end_dt), self.oracle_connection_string)
		df = oracle_reader.process(self.cardo_context).dataframe
		return [row[DT] for row in df.select(DT).collect()]
	
	def _create_current_dts_list(self):
		current_datetime = self.start_datetime
		dt_list = []
		while current_datetime < self.end_datetime:
			dt_list.append(current_datetime.strftime(SKIPPY_DT_FORMAT))
			current_datetime += self.period
		return dt_list
	
	def _get_previous_dt(self):
		return (self.start_datetime - self.period).strftime(SKIPPY_DT_FORMAT)
