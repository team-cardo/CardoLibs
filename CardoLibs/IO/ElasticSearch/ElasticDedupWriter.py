import pyspark.sql.functions as F

from py4j.protocol import Py4JJavaError

from CardoLibs.IO.ElasticSearch.ElasticWriter import ElasticWriter

ES_MAPPING_ID = "es.mapping.id"
ES_OPERATION = "es.write.operation"
ES_UPDATE_SCRIPT = "es.update.script"
UPDATE_OPERATIONS = ["update", "upsert"]
ES_FORMAT = 'org.elasticsearch.spark.sql'
ES_SCRIPT_PARAMS = "es.update.script.params"
TAKE_MIN_SCRIPT = u"""ctx._source.{column} = ctx._source.{column} == null || ctx._source.{column}.compareTo(params.{param}) > 0 ? params.{param} : ctx._source.{column};"""
TAKE_MAX_SCRIPT = u"""ctx._source.{column} = ctx._source.{column} == null || ctx._source.{column}.compareTo(params.{param}) < 0 ? params.{param} : ctx._source.{column};"""
TAKE_NEW_SCRIPT = u"""ctx._source.{column} = params.{param} != null ? params.{param} : ctx._source.{column};"""
GENERIC_PARAM = "{param}:{column}"
PARAM_TEMPLATE = '{column}_param'
ELASTIC_INSERTION_TIME_COLUMN = '_es_insertion_time'
ELASTIC_INSERTION_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

class ElasticDedupWriter(ElasticWriter):
    def __init__(self, first_seen, last_seen, insertion_time=ELASTIC_INSERTION_TIME_COLUMN, operation="upsert", options=None,
                 take_min_columns=None, take_max_columns=None, take_new_columns=None, **kwargs):
        super(ElasticDedupWriter, self).__init__(**kwargs)
        self.take_new_columns = take_new_columns or []
        self.take_min_columns = (take_min_columns or []) + [first_seen]
        self.take_max_columns = (take_max_columns or []) + [last_seen, insertion_time]
        self.insertion_time = insertion_time
        self.__create_options(operation, options)


    def _generate_script(self, base):
        return base.replace('\n', '').format(**vars(self))

    def _add_insertion_time(self, cardo_dataframe):
        cardo_dataframe.dataframe = cardo_dataframe.dataframe.withColumn(self.insertion_time,
                                                                         F.date_format(F.current_timestamp(), ELASTIC_INSERTION_TIME_FORMAT))

    def _write(self, cardo_context, cardo_dataframe):
        try:
            self._add_insertion_time(cardo_dataframe)
            if self.options[ES_OPERATION] not in UPDATE_OPERATIONS and ES_MAPPING_ID in self.options:
                self.options.pop(ES_MAPPING_ID)
            cardo_dataframe.dataframe.write.save(self._location(), ES_FORMAT, mode='append', **self.options)
            cardo_context.logger.info("Wrote data to index {}".format(self._location()))
        except (IOError, Py4JJavaError):
            cardo_context.logger.error('failed to write {} to index {}'.format(cardo_dataframe.table_name,
                                                                               self._location()))
            raise

    def __get_parameter_and_script(self, column):
        param_name = PARAM_TEMPLATE.format(column=column)
        param = GENERIC_PARAM.format(column=column, param=param_name)
        if column in self.take_min_columns:
            return param, TAKE_MIN_SCRIPT.format(column=column, param=param_name)
        if column in self.take_max_columns:
            return param, TAKE_MAX_SCRIPT.format(column=column, param=param_name)
        if column in self.take_new_columns:
            return param, TAKE_NEW_SCRIPT.format(column=column, param=param_name)

    def __get_mapping_script_and_params(self):
        scripts = []
        params = []
        for column in set(self.take_min_columns + self.take_max_columns + self.take_new_columns):
            param, script = self.__get_parameter_and_script(column)
            scripts.append(script)
            params.append(param)
        return ''.join(scripts), ','.join(params)

    def __create_options(self, operation, options=None):
        mapping_script, mapping_params = self.__get_mapping_script_and_params()
        self.options = {
            ES_MAPPING_ID: self.primary_key,
            ES_OPERATION: operation,
            ES_SCRIPT_PARAMS: mapping_params,
            ES_UPDATE_SCRIPT: mapping_script
        }
        if options:
            self.options.update(options)
