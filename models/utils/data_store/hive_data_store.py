from utils.data_store.abstract_data_store import AbstractDataStore
from functools import reduce
import pandas as pd
import json
import urllib.request
import numpy as np
from config import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from statistics import mean, stdev



class HiveDataStore(AbstractDataStore):
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

    def read_spark_df_from_itd_data_store(self, **args):
        """
        Read table into SPARK dataframe

        TODO complete docstring

        """
        colnames = args["numerical_colnames"]
        colnames_string = ",".join(colnames)
        database = args["tenant_name"]
       
        query = "select " + colnames_string + " from " + database + "." + table + " limit 50000"
        sdf = self.spark.sql(query)
        for column in colnames:
            sdf = sdf.withColumn(column, sdf[column].cast(IntegerType()))
        return sdf

    def read_spark_df_from_windowsnxlog_data_store(self,**args):
        year = args["year"]
        month = args["month"]
        day = args["day"]
        conn = hive.Connection(host='10.10.110.96', port=10500, username='hdfs',database='demo')
        cur = conn.cursor()
        cur.execute("set hive.resultset.use.unique.column.names=false")
        d = pd.read_sql_query("select * from windowsnxlog_ml_user_aggregation where y="+ str(year) + " and m="+str(month)+" and d="+str(day) ,conn)
        d = d.to_json(orient='records')
        res = json.loads(d) 
        spark = SparkSession.builder.appName('mseapi').enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        tsRDD = sc.parallelize(res)
        df_nxlog_user = spark.read.option('multiline', "true").json(tsRDD)
        total_evt_count = df_nxlog_user.count()
        print("Total number of records: " + str(total_evt_count))
        return df_nxlog_user
		
		
		
    def read_spark_df_from_win_data_store(self,**args):
        year = args["year"]
        month = args["month"]
        day = args["day"]
        table_name = args["table_name"]	
        database = args["database"]
        query = "select * from "+ database + "." + table_name +" where  y="+str(year)+ " and m="+str(month)+ " and d="+str(day) 
        sdf = self.spark.sql(query)
        sdf.show(3)
        return sdf
    