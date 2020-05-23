import json

from kafka import KafkaConsumer
from pyspark.sql.types import *

from utils.data_store.abstract_data_store import AbstractDataStore
from config import *

class KafkaDataStore(AbstractDataStore):

    def __init__(self, spark):
        self.spark = spark
        return

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

        database = args["tenant_name"]
        table = args["data_source"]
        broker_list = args["broker_list"]
        kafka_topic = args["kafka_topic"]
        kafka_group_id = args["kafka_group_id"]
        source = database + "_" + table
        data_list = list()
        numerical_colnames = args["numerical_colnames"]
        categorical_colnames = args["categorical_colnames"]
        consumer = KafkaConsumer(kafka_topic,
                                 group_id=kafka_group_id,
                                 bootstrap_servers=broker_list,
                                 auto_offset_reset='smallest', enable_auto_commit=True)
        i = 0
        for message in consumer:
            mess = message.value.decode('ascii', 'ignore')
            data_dict = json.loads(mess)
            source_type = data_dict["source.type"]
            if source_type == source:
                i += 1
                temp_list = list()
                for num_colname in numerical_colnames:
                    val = 0.0
                    if num_colname in data_dict:
                        val = float(data_dict[num_colname])
                    temp_list.append(val)
                for cat_colname in categorical_colnames:
                    val = ""
                    if cat_colname in data_dict:
                        val = str(data_dict[cat_colname])
                    temp_list.append(val)
                data_list.append(temp_list)
            if i == MAX_EVENT_RECORD_COUNT:
                break

        rdd = self.spark.sparkContext.parallelize(data_list)
        struct_type = [StructField(num_colname, FloatType(), True) for num_colname in numerical_colnames] + [
            StructField(cat_colname, StringType(), True) for cat_colname in categorical_colnames]
        schema = StructType(struct_type)
        sdf = self.spark.createDataFrame(rdd, schema)
        sdf.show(3)
        return sdf
