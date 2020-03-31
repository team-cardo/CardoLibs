from datetime import datetime
from six import text_type

from CardoExecutor.Common import CardoDataFrame, CardoContext
from CardoExecutor.Contract.IStep import IStep
from py4j.protocol import Py4JJavaError

from CardoLibs.IO.ElasticSearch.ElasticConnector import ElasticConnector

ELASTIC_FORMAT = 'org.elasticsearch.spark.sql'
ELASTIC_SPARK_NODES = 'spark.es.nodes'
ELASTIC_MAPPING_ID = "es.mapping.id"
ELASTIC_DEFAULT_PORT = 9200


def default_suffix():
	# type: () -> text_type
	time_format = "%Y-%m"  # year and month, e.g. 2018-09
	return text_type(datetime.now().strftime(time_format))


class ElasticWriter(IStep):
	def __init__(self, template_name, type_name, port=ELASTIC_DEFAULT_PORT, suffix_function=default_suffix,
				 override_mapping=None, primary_key=None, use_alias=True, timeout=90, update_template=True, connector_kwargs=None):
		self.connector_kwargs = connector_kwargs or {}
		self.update_template = update_template
		self.port = port
		self.timeout = timeout
		self.doc_type = type_name
		self.template_name = template_name
		self.suffix_function = suffix_function
		self.override_mapping = override_mapping
		self.primary_key = primary_key
		self.use_alias = use_alias

	def process(self, cardo_context, cardo_dataframe):
		# type: (CardoContext, CardoDataFrame) -> None
		self._set_up_index(cardo_dataframe)
		cardo_dataframe = self._write(cardo_context, cardo_dataframe)
		return cardo_dataframe

	def _write(self, cardo_context, cardo_dataframe):
		# type: (CardoContext, CardoDataFrame) -> None
		options = {}
		if self.primary_key is not None:
			options[ELASTIC_MAPPING_ID] = self.primary_key
		try:
			cardo_dataframe.dataframe.write.save(self._location(), ELASTIC_FORMAT, mode='append', **options)
			cardo_context.logger.info("Wrote data to index {}".format(self._location()))
		except (IOError, Py4JJavaError):
			cardo_context.logger.error('failed to write {} to index {}'.format(cardo_dataframe.table_name,
																			   self._location()))
			raise
		return cardo_dataframe

	def _index_name(self, private=True):
		private = "-private" if private and self.use_alias else ""
		return "{template}-{suffix}{private}".format(template=self.template_name, suffix=self.suffix_function(),
													 private=private)

	def _location(self):
		return "{index}/{doc_type}".format(index=self._index_name(), doc_type=self.doc_type)

	def _create_connection(self, cardo_dataframe):
		hosts = cardo_dataframe.dataframe._sc.getConf().get(ELASTIC_SPARK_NODES).split(",")
		return ElasticConnector(hosts, self.port, self.timeout, **self.connector_kwargs)

	def _set_up_index(self, cardo_dataframe):
		conn = self._create_connection(cardo_dataframe)
		schema = cardo_dataframe.dataframe.schema
		if self.update_template:
			conn.update_template(self.template_name, self.doc_type, schema, self.override_mapping)
		conn.create_index(self._index_name())
		if self.use_alias:
			conn.create_alias(self._index_name(), alias=self._index_name(private=False))
