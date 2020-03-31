import re
from CardoExecutor.Contract.CardoContextBase import CardoContextBase


class JDBCQuerier:
	def __init__(self, cardo_context, connection_string, driver_name):
		# type: (CardoContextBase, str, str) -> JDBCQuerier
		self.cardo_context = cardo_context
		self.connection_string = connection_string
		self.driver_name = driver_name

	def __enter__(self):
		self.driver = getattr(self.cardo_context.spark._jvm, self.driver_name)()
		self.connection = self.driver.connect(self.connection_string, None)
		self.cursor = self.connection.createStatement()
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.connection.close()

	def __format_query(self, query):
		return re.sub('\s+', ' ', query)

	def execute(self, query):
		formatted_query = self.__format_query(query)
		return self.cursor.executeQuery(formatted_query)
