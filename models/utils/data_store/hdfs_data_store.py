from utils.data_store.abstract_data_store import AbstractDataStore


class HDFSDataStore(AbstractDataStore):
    def __init__(self, spark):
        """
        TODO complete docstring
        :param src_dir:
        """
        self.spark = spark
        # ensure path ends with a forward slash

    def read_pandas_df_from_data_store(self, **args):
        """
        Read table into PANDAS dataframe

        TODO complete docstring

        """
        return

    def read_spark_df_from_data_store(self, **args):
        """
        Read table into SPARK dataframe

        TODO complete docstring

        """
        data_path = args["data_path"]
        sdf = self.spark.read.json(data_path).persist()
        return sdf
