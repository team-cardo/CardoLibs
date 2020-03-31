import datetime

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IRecycleStep import IRecycleStep
from CardoExecutor.Contract.RecycleStepRuntimeWrapper import RecycleStepRuntimeWrapper, SAVE_RESULTS_AT_RUNTIME_CONFIG, \
    loaded_steps, DEFAULT_SCHEMA, HIVE_SCHEMA_CACHE_CONFIG, LOG_TEMPLATE, TIMESTAMP_FORMAT, FULL_TABLE_NAME_TEMPLATE
from CardoExecutor.Contract.IStep import step_to_hash_dict, hash_to_df_dict

from CardoLibs.IO import HiveWriter


class BuildDataset(IRecycleStep):
    def __init__(self, index_col, name, recompute=False, load_delta_limit=None, rename=None):
        super(BuildDataset, self).__init__(name, recompute, load_delta_limit, rename)
        self.index_col = index_col

    def process(self, cardo_context, *cardo_dataframes):
        # type: (CardoContextBase, [CardoDataFrame]) -> CardoDataFrame
        assert len(cardo_dataframes) > 0
        result = self._rename_columns(cardo_dataframes[0])
        for cardo_dataframe in cardo_dataframes[1:]:
            df = self._rename_columns(cardo_dataframe)
            result = result.join(df, self.index_col, how='outer')
        hash_to_df_dict[step_to_hash_dict[self]] = result
        if cardo_context.spark.conf.get(SAVE_RESULTS_AT_RUNTIME_CONFIG, 'True') != 'True':
            self._save_all_steps_results(cardo_context)
        return CardoDataFrame(result)

    def _rename_columns(self, cardo_dataframe):
        df = cardo_dataframe.dataframe
        for col in (set(df.columns) - {self.index_col}):
            df = df.withColumnRenamed(col, '{table_name}_{col}'.format(table_name=cardo_dataframe.table_name, col=col))
        return df

    @staticmethod
    def _save_all_steps_results(cardo_context):
        for step in step_to_hash_dict.keys() - loaded_steps:
            schema_name = cardo_context.spark.conf.get(HIVE_SCHEMA_CACHE_CONFIG, DEFAULT_SCHEMA)
            table_name_template = DSIStepRuntimeWrapper.step_table_name_template(step)
            timestamp = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
            table_name_to_save = FULL_TABLE_NAME_TEMPLATE.format(ds_schema=schema_name,
                                                                 table_name_template=table_name_template,
                                                                 timestamp=timestamp)
            HiveWriter(table_name_to_save).process(cardo_context, hash_to_df_dict[step_to_hash_dict[step]])
            cardo_context.logger.info(
                'Saved ' + LOG_TEMPLATE.format(step_name=step.name, hash=step_to_hash_dict[step].hex(),
                                               step_params=step.__dict__, ds_schema=schema_name,
                                               table_name=table_name_to_save))
