from multiprocessing.pool import ThreadPool

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep

from CardoLibs.IO.ElasticSearch.ElasticWriter import ElasticWriter, ELASTIC_FORMAT
from elasticsearch import Elasticsearch


class ElasticUnioner(IStep):
    def __init__(self, host, index, type, parallel):
        self.host = host
        self.index = index
        self.type = type
        self.parallel = parallel

    def __write_dataframe_to_elastic(self, cardo_context, cardo_dataframe):
        cardo_dataframe.dataframe.write.save('{}/{}'.format(self.index, self.type), ELASTIC_FORMAT, mode='append')

    def __read_from_elastic(self, cardo_context):
        index_url = '{index}/{type}'.format(index=self.index, type=self.type)
        return cardo_context.spark.read.format('es').load(index_url)

    def process(self, cardo_context, *cardo_dataframes):
        pool = ThreadPool(self.parallel)
        Elasticsearch(self.host).indices.delete(index=self.index, ignore=[400, 404])
        pool.map(lambda cardo_dataframe: self.__write_dataframe_to_elastic(cardo_context, cardo_dataframe),
                 cardo_dataframes)
        unioned = self.__read_from_elastic(cardo_context)
        return CardoDataFrame(unioned, 'unioned')
