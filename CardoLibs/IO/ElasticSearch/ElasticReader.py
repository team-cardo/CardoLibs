# -*- coding:utf-8 -*-
from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep
from pyspark.sql import readwriter

ELASTIC_FORMAT = 'org.elasticsearch.spark.sql'
ELASTIC_SPARK_NODES = 'es.nodes'
ELASTIC_SPARK_INDEX = "es.resource"
ELASTIC_SPARK_QUERY = "es.query"
DEFAULT_QUERY = """{
  "query": {
    "match_all": {}
  }
}"""


class ElasticReader(IStep):
	def __init__(self, index, hosts, type_name=None, query=DEFAULT_QUERY, options=None):
		if isinstance(hosts, list):
			hosts = ",".join(hosts)
		self.resource = "{}/{}".format(index, type_name) if type_name else index
		self.options_dict = {ELASTIC_SPARK_INDEX: self.resource, ELASTIC_SPARK_NODES: hosts, ELASTIC_SPARK_QUERY: query}
		if options:
			self.options_dict.update(options)

	def process(self, cardo_context, cardo_dataframe=None):
		# type: (CardoContext) -> CardoDataFrame
		cardo_context.logger.info("Reading data from: {} index".format(self.resource))
		function_keeper = self.override_spark_to_str()
		df = cardo_context.spark.read.format(ELASTIC_FORMAT).options(**self.options_dict).load()
		self.return_to_str_to_original_function(function_keeper)
		cardo_context.logger.info("Read data from: {} index successfully".format(self.resource))
		return CardoDataFrame(df)

	def override_spark_to_str(self):
		function_keeper = readwriter.to_str
		readwriter.to_str = lambda x: x
		return function_keeper

	def return_to_str_to_original_function(self, function_keeper):
		readwriter.to_str = function_keeper
