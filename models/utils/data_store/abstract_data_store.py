from abc import abstractmethod


class AbstractDataStore(object):


    @abstractmethod
    def read_pandas_df_from_data_store(self, **args):
        """
        Read table into PANDAS dataframe

        TODO complete docstring

        """
        return

    @abstractmethod
    def read_spark_df_from_data_store(self, **args):
        """
        Read table into SPARK dataframe

        TODO complete docstring

        """
        return