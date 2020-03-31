from CardoExecutor.Contract.IStep import IStep


class FunctionStep(IStep):
    """
    If someone need step that does only one small action so he doesnt have to
    create new IStep, he can just use this step.

    Usage Example:
        subworkflow.add_last(FunctionStep(
            lambda context, cardo_df: CardoDataFrame(cardo_df.dataframe.withColumnRenamed(
                "dt", "source_table_insertion_time"))))
    """

    def __init__(self, func, kwargs=None, **kwargs2):
        super(IStep, self).__init__(**kwargs2)
        self.kwargs = kwargs or {}
        self.function = func

    def process(self, cardo_context, *cardo_dataframes):
        return self.function(cardo_context, *cardo_dataframes, **self.kwargs)
