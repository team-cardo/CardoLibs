from CardoExecutor.Contract.IStep import IStep
from lxml import etree
from pyspark.sql.functions import first


class ParseXML(IStep):
    def __init__(self, xml_col, hash_column, repeated_tag, clean_regex=None, save_previous_columns=False,
                 delimiter='-', encoding='utf-8', save_prev_cols=False):
        # type: (str, str, str, str, bool, str, bool)-> None
        self.xml_col = xml_col
        self.hash_column = hash_column
        self.delimiter = delimiter
        self.clean_regex = clean_regex
        self.new_hash_column = "_1"
        self.names_column = "_2"
        self.values_column = "_3"
        self.repeated_tag = repeated_tag
        self.save_previous_columns = save_previous_columns
        self.encoding = encoding
        self.save_prev_cols = save_prev_cols
        self.very_long_string_length = 10000000
        self.hash_prefix_length = 3

    def process(self, cardo_context, cardo_dataframe):
        # type: (CardoContext, CardoDataFrame) -> CardoDataFrame
        df = cardo_dataframe.dataframe
        rdd = df.rdd
        try:
            parsed_rdd = rdd.flatMap(lambda row: self.__parse_xml(row))
            parsed_df = parsed_rdd.toDF()
        except ValueError:
            cardo_context.logger.info("DF is empty")
            return cardo_dataframe
        if self.clean_regex is not None:
            parsed_df = parsed_df.replace(self.clean_regex, '', subset=self.names_column)
        parsed_df = parsed_df.groupBy(self.new_hash_column).pivot(self.names_column).agg(first(self.values_column))
        parsed_df = self.__save_prev_columns(df, parsed_df)
        cardo_dataframe.dataframe = parsed_df
        return cardo_dataframe

    def __save_prev_columns(self, df, parsed_df):
        # type: (DataFrame, DataFrame) -> DataFrame
        if self.save_prev_cols:
            parsed_df = parsed_df.withColumn(self.hash_column,
                                             parsed_df[self.new_hash_column].substr(self.hash_prefix_length,
                                                                                    self.very_long_string_length))
            parsed_df = parsed_df.join(df, on=[self.hash_column], how="left").drop(self.xml_col, self.hash_column)
        return parsed_df

    def __parse_xml(self, row):
        # type: (Row)-> [str]
        results = []
        if row[self.xml_col] is not None:
            xml = row[self.xml_col].encode(self.encoding)
            parser = etree.XMLParser(ns_clean=True, recover=True, encoding=self.encoding)
            parsed_xml = etree.fromstring(xml, parser=parser)
            sources = [[tag] for tag in parsed_xml]
            index = 0
            if self.save_previous_columns:
                self.__save_prev_cols(index, results, row)
            while len(sources) > 0:
                curr_elem_path = sources.pop()
                curr_elem = curr_elem_path[-1]
                if self.repeated_tag == curr_elem.tag:
                    index += 1
                if curr_elem.attrib != {}:
                    self.__add_element_to_results(curr_elem, curr_elem_path, index, results, row)
                self.__add_childern_of_attrib_to_results(curr_elem, curr_elem_path, index, results, row, sources)
        return results

    def __save_prev_cols(self, index, results, row):
        # type: (int, [str], Row)-> None
        columns = [col for col in row.__fields__ if col != self.xml_col and col != self.hash_column]
        for column in columns:
            results.append((str(index) + self.delimiter + row[self.hash_column], column, row[column]))

    def __add_element_to_results(self, curr_elem, curr_elem_path, index, results, row):
        # type: (list, [str], int, [str], Row)-> None
        concat_column = self.delimiter.join([t.tag for t in curr_elem_path]) + self.delimiter + \
                        curr_elem.attrib.keys()[0]
        results.append(
            (str(index) + self.delimiter + row[self.hash_column], concat_column, curr_elem.attrib.values()[0]))

    def __add_childern_of_attrib_to_results(self, curr_elem, curr_elem_path, index, results, row, sources):
        # type: (list, [str], int, [str], Row, [[str]])-> None
        if len(curr_elem.getchildren()) == 0:
            if len(curr_elem.text) > 0:
                concat_column = self.delimiter.join([t.tag for t in curr_elem_path])
                results.append((str(index) + self.delimiter + row[self.hash_column], concat_column, curr_elem.text))
        else:
            for t in curr_elem.getchildren():
                sources.append(curr_elem_path + [t])
