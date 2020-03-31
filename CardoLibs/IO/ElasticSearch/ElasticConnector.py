from elasticsearch import Elasticsearch
from six import text_type, iteritems
from pyspark.sql.types import DataType, StructType

BASE_TEMPLATE_BODY = {u"template": u"{name}-",
                      u"setting": {u"index": {u"number_of_shards": u"5", u"number_of_replicas": u"1"}}}
spark_es_mapping = {u"string": u"text", u"int": u"long", u"decimal": u"double", u"double": u"double",
                    u"float": u"double", u"byte": u"double", u"integer": u"long", u"long": u"long", u"short": u"long",
                    u"array": u"array", u"map": u"object", u"date": u"date", u"timestamp": u"date"}


class ElasticConnector(object):
    def __init__(self, hosts, port, timeout, **kwargs):
        self.conn = Elasticsearch(hosts=hosts, port=port, **kwargs)
        self.timeout = timeout

    def update_template(self, name, type_name, schema, override_mapping=None):
        template = self.get_or_create_template(name)
        mapping = _get_mapping_setting(type_name, schema, override_mapping or {})
        template.update(mapping)
        self.conn.indices.put_template(name, template, request_timeout=self.timeout)

    def get_or_create_template(self, name):
        if not self.conn.indices.exists_template(name, request_timeout=self.timeout):
            body = dict(iteritems(BASE_TEMPLATE_BODY))
            body[u"template"] = body[u"template"].format(name=name)
            self.conn.indices.put_template(name, body, request_timeout=self.timeout)
        return self.conn.indices.get_template(name, request_timeout=self.timeout).get(name)

    def create_index(self, index):
        if not self.conn.indices.exists(index):
            self.conn.indices.create(index, request_timeout=self.timeout)

    def create_schema_index(self, index, doc_type, table_name, columns):
        if not self.conn.indices.exists(index):
            self.conn.indices.create(index, request_timeout=self.timeout)
        self.conn.index(index, doc_type, {u"fields": columns, u"id": table_name})

    def create_alias(self, index, alias):
        if not self.conn.indices.exists_alias(index, alias):
            self.conn.indices.put_alias(index=index, name=alias, request_timeout=self.timeout)


def _get_mapping_setting(type_name, df_schema, override_mapping):
    # type: (str, StructType, dict) -> dict
    mapping = _schema_to_mapping(df_schema.fields)
    mapping.update(override_mapping)
    return {u"mappings": {type_name: {u"properties": mapping}}}


def _schema_to_mapping(df_schema):
    elastic_mapping = {}
    for field in df_schema:
        elastic_mapping[field.name] = _spark_to_elastic_typing(_clean_schema_name(field.dataType))
    return elastic_mapping


def _clean_schema_name(data_type):
    # type: (DataType) -> text_type
    return text_type(data_type).replace(u"Type", u"").lower()


def _spark_to_elastic_typing(spark_type):
    return {u"type": spark_es_mapping.get(spark_type, spark_type)}
