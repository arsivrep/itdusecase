import numpy as np
import json
import urllib.request
from functools import reduce
from pyspark.sql.types import *
import pandas as pd
from utils.data_store.abstract_data_store import AbstractDataStore

import json
import urllib.request
from functools import reduce
from pyspark.sql.types import *
import pandas as pd
from utils.data_store.abstract_data_store import AbstractDataStore
import sys
import time
import requests
import argparse
import numpy as np
from config import *
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
import pyspark.sql.functions as f
from pyspark.sql.types import * 
from statistics import mean, stdev
from utils.data_store.abstract_data_store import AbstractDataStore
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 1000)
# pd.set_option('expand_frame_repr', True)
pd.set_option('max_colwidth', 1000)


class HBaseDataStore(AbstractDataStore):

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
        url = args["hbase_url"]
        numerical_colnames = args["numerical_colnames"]
        actor_type = args["actor_type"]
        colname_to_content_dict = dict()
        categorical_colnames = args["categorical_colnames"]
        logger = args["logger"]

        for numerical_colname in numerical_colnames:
            content_url = url.replace("$attribute_name$", numerical_colname)
            logger.info(content_url)
            contents = urllib.request.urlopen(content_url).read()
            data = json.loads(contents)
            df = pd.DataFrame(list(data.items()), columns=[actor_type, numerical_colname])
            colname_to_content_dict[numerical_colname] = df

        for categorical_colname in categorical_colnames:
            content_url = url.replace("$attribute_name$", categorical_colname)
            logger.info(content_url)
            contents = urllib.request.urlopen(content_url).read()
            data = json.loads(contents)
            df = pd.DataFrame(list(data.items()), columns=[actor_type, categorical_colname])
            colname_to_content_dict[categorical_colname] = df

        dfs = colname_to_content_dict.values()
        df = reduce(lambda left, right: pd.merge(left, right, on=actor_type), dfs)
        struct_field_list = list()
        struct_field_list.append(StructField(args["actor_type"], StringType(), True))
        for numerical_colname in numerical_colnames:
            struct_field_list.append(StructField(numerical_colname, FloatType(), True))
        for categorical_colname in categorical_colnames:
            struct_field_list.append(StructField(categorical_colname, StringType(), True))
        schema = StructType(struct_field_list)
        sdf = self.spark.createDataFrame(df, schema)
        return sdf

    def read_spark_df_from_msexchange_data_store(self,**args):
        url = args["hbase_url"]
       
        r = requests.get(url)

        # Converting api data in json file
        try: 
            d = r.json()
          
        except:
            print ("Invalid URL")

        # Checking for data availability
        if len(d) == 0 :
            print("There are no events to process. Please enter a different search criteria in the url.")

        # Converting API data into Spark Dataframe
        print("Reading the data from profiler...")
        spark = SparkSession.builder.appName('mseapi').enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        tsRDD = sc.parallelize(d)
        df_mail = spark.read.option('multiline', "true").json(tsRDD)
        total_evt_count = df_mail.count()
        print("Total number of records: " + str(total_evt_count))


        if total_evt_count > 0 :
            mail_len   = f.udf(lambda s: len(s), LongType())
            mail_sum   = f.udf(lambda s: sum(s), LongType())
            # mail_mean  = f.udf(lambda s: round(mean(s),4), FloatType())
            # mail_stdev = f.udf(lambda s: round(stdev(s),4), FloatType())


            df_mail_grp = df_mail.filter(f.length(f.trim(df_mail["mail_size"]))>0)\
                            .withColumn("check", f.when(f.instr(df_mail["mail_size"],',') == 1,f.substring_index(df_mail["mail_size"],',',-1)).otherwise(df_mail["mail_size"]))\
                            .withColumn("ext_sndrs", df_mail["ext_sndrs"].cast(LongType()))\
                            .withColumn("mail_size", f.regexp_replace('check', ' ', ''))\
                            .groupBy(["mail_id"]).agg(f.split(f.concat_ws(",", f.collect_list("mail_size")),',')
                                                            .cast(ArrayType(IntegerType())).alias("email_size"), 
                                                    f.sum("ext_sndrs").alias("ext_sndrs"))\
                            .withColumn("no_of_emails", mail_len("email_size"))\
                            .withColumn("tot_email_size", mail_sum("email_size"))\
                            .withColumn("avg_email_size", f.round(f.col("tot_email_size")/ f.col("no_of_emails"),4))\
                            .drop("email_size")
                            #.withColumn("email_size_mean", mail_mean("email_size"))\
                            #.withColumn("email_size_stdev", f.when(mail_len("email_size") > 1,mail_stdev("email_size")))\				



            # df_mail_grp = df_mail.filter(f.length(f.trim(df_mail["mail_size"]))>0)\
            #                 .withColumn("check", f.when(f.instr(df_mail["mail_size"],',') == 1,f.substring_index(df_mail["mail_size"],',',-1)).otherwise(df_mail["mail_size"]))\
            #                 .withColumn("ext_sndrs", df_mail["ext_sndrs"].cast(LongType()))\
            #                 .withColumn("mail_size", f.regexp_replace('check', ' ', ''))\
            #                 .groupBy(["mail_id"]).agg(f.split(f.concat_ws(",", f.collect_list("mail_size")),',')
            #                                                 .cast(ArrayType(IntegerType())).alias("email_size"), 
            #                                         f.sum("ext_sndrs").alias("ext_sndrs"))\
            #                 .withColumn("no_of_emails", mail_len("email_size"))\
            #                 .withColumn("tot_email_size", mail_sum("email_size"))\
            #                 .withColumn("avg_email_size", f.round(f.col("tot_email_size")/ f.col("no_of_emails"),4))\
            #                 .drop("email_size")
            #                 #.withColumn("email_size_mean", mail_mean("email_size"))\
            #                 #.withColumn("email_size_stdev", f.when(mail_len("email_size") > 1,mail_stdev("email_size")))\




            # df_mail_grp = df_mail.withColumn("ext_sndrs", df_mail["ext_sndrs"].cast(LongType()))\
            #                     .withColumn("mail_size", f.regexp_replace('mail_size', ' ', ''))\
            #                     .groupBy(["mail_id"]).agg(f.split(f.concat_ws(",", f.collect_list("mail_size")),',')
            #                                                     .cast(ArrayType(IntegerType())).alias("email_size"), 
            #                                             f.sum("ext_sndrs").alias("ext_sndrs"))\
            #                     .withColumn("no_of_emails", mail_len("email_size"))\
            #                     .withColumn("tot_email_size", mail_sum("email_size"))\
            #                     .withColumn("email_size_mean", mail_mean("email_size"))\
            #                     .withColumn("email_size_stdev", mail_stdev("email_size"))\
            #                     .drop("email_size")
            df_mail_grp.show(3)
            return df_mail_grp

        else :
            schema = StructType([])
            sqlContext = SQLContext(sc)
            sdf = sqlContext.createDataFrame(sc.emptyRDD(), schema)
            return sdf
			
			
    def read_spark_df_from_windowsnxlog_user_data_store(self,**args):
        url = args["hbase_url"]
   
        r = requests.get(url)
        actor_type = args["actor_type"]
        

        # Converting api data in json file
        try: 
            d = r.json()
            
        except:
            print ("Invalid URL")

        # Checking for data availability
        if len(d) == 0 :
            print("There are no events to process. Please enter a different search criteria in the url.")

        # Converting API data into Spark Dataframe
        print("Reading the data from profiler...")
        spark = SparkSession.builder.appName('mseapi').enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        tsRDD = sc.parallelize(d)
        df_nxlog_user = spark.read.option('multiline', "true").json(tsRDD)
        total_evt_count = df_nxlog_user.count()
        print("Total number of records: " + str(total_evt_count))


        if total_evt_count > 0 :
            df_nxlog_user_grp = df_nxlog_user.withColumn("username", f.lower(df_nxlog_user["username"]))\
                                .groupBy(["username"]).agg(f.sum("failedlogin_count").cast(LongType()).alias("failedlogin_count"),
                                                  f.sum("fileactivity_count").cast(LongType()).alias("fileactivity_count"),
                                                  f.sum("logins_count").cast(LongType()).alias("logins_count"),
                                                  f.sum("other_scripts_count").cast(LongType()).alias("other_scripts_count"),
                                                  f.sum("pri_count").cast(LongType()).alias("pri_count"),
                                                  f.sum("ps_count").cast(LongType()).alias("ps_count")) 
            df_nxlog_user_grp.show(3)
            return df_nxlog_user_grp

        else :
            schema = StructType([])
            sqlContext = SQLContext(sc)
            sdf = sqlContext.createDataFrame(sc.emptyRDD(), schema)
            return sdf      


    def read_spark_df_from_windowsnxlog_entity_data_store(self,**args):
        url = args["hbase_url"]
      
        r = requests.get(url)
        actor_type = args["actor_type"]
        # Converting api data in json file
        try: 
            d = r.json()
           
        except:
            print ("Invalid URL")

        # Checking for data availability
        if len(d) == 0 :
            print("There are no events to process. Please enter a different search criteria in the url.")

        # Converting API data into Spark Dataframe
        print("Reading the data from profiler...")
        spark = SparkSession.builder.appName('mseapi').enableHiveSupport().getOrCreate()
        sc = spark.sparkContext
        tsRDD = sc.parallelize(d)
        df_nxlog_entity = spark.read.option('multiline', "true").json(tsRDD)
        total_evt_count = df_nxlog_entity.count()
        print("Total number of records: " + str(total_evt_count))


        if total_evt_count > 0 :
            df_nxlog_entity_grp = df_nxlog_entity.groupBy(["ip_src_addr"]).agg(f.sum("failedlogin_count").cast(LongType()).alias("failedlogin_count"),
                                                  f.sum("fileactivity_count").cast(LongType()).alias("fileactivity_count"),
                                                  f.sum("logins_count").cast(LongType()).alias("logins_count"),
                                                  f.sum("other_scripts_count").cast(LongType()).alias("other_scripts_count"),
                                                  f.sum("pri_count").cast(LongType()).alias("pri_count"),
                                                  f.sum("ps_count").cast(LongType()).alias("ps_count")) 
            df_nxlog_entity_grp.show(3)
            return df_nxlog_entity_grp

        else :
            schema = StructType([])
            sqlContext = SQLContext(sc)
            sdf = sqlContext.createDataFrame(sc.emptyRDD(), schema)
            return sdf          			
			
			
			
			

