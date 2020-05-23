#Importing Libraries
from pyspark.sql.functions import lit
import argparse
import datetime
import time
import json
import ssl
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext, Row

#Importing Modules
from config import *
from model_platform.src.model.anomaly.profile_model.cluster_profile_model import \
    ClusterProfileModel
from model_platform.src.model.anomaly.profile_model.isolation_forest_profile_model import \
    IsolationForestProfileModel
from model_platform.src.model.anomaly.profile_model.oneclasssvm_profile_model import \
    OneClassSVMProfileModel
from utils.data_store.hive_data_store import HiveDataStore
from utils.logging.notify_error import send_error_notification
from utils.logging.pylogger import get_log_path, configure_logger

#Funtion Definitions
def calc_zscore(colname,val,mean,stdev):
    dict_val = {}
    dict_val[colname] = round((val-mean)/stdev, 4 )
    return dict_val


def write_spark_df_to_hdfs(spark, output_schema, database, table_name, sdf, timestamp):
    hive_context = HiveContext(spark.sparkContext)
    hive_context.setConf("hive.exec.dynamic.partition", "true")
    hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
    sdf=sdf.withColumn("execution_timestamp",lit(timestamp))
    sdf = sdf.na.fill(0.0, subset=["pas_kmeans", "pas_isolation", "pas_svm", "pas"])
    sdf.select(output_schema).write.insertInto(database + "." + table_name)




def score_profile_anomaly_model(spark, model_dict, data_frame, numerical_colnames, categorical_colnames, actor_type, date_str, timestamp, logger):
    input_sdf = data_frame 
    if input_sdf.rdd.isEmpty():
        return None
    logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")        
    logger.info("Step 1:::::Conveting to Pandas DF")  
    input_df = input_sdf.toPandas()
    stat_dict = {}  
    for colname in numerical_colnames:
        stat_dict[colname] = [input_df[[colname]].mean()[0],input_df[[colname]].std()[0]]
    input_df_zsc = input_df.copy()
    logger.info("Step 2:::::Calculating z-scores")    
    input_df_zsc['zscore'] =  input_df_zsc.apply (lambda row: "'" + str([calc_zscore(col, row[col],stat_dict[col][0],stat_dict[col][1]) for col in numerical_colnames]) + "'", axis= 1)
    
    logger.info("Step 3:::::Scoring for K-Means Model Started") 
    result_df_1 = model_dict[date_str][actor_type][PYSPARK_KMEANS_MODEL].score(
        input_sdf).toPandas()
    input_df_zsc["pas_kmeans"] = result_df_1["PAS"]
    input_df_zsc["pas_kmeans"] = input_df_zsc["pas_kmeans"].round(4)
    
    logger.info("Step 4:::::Scoring for Isolation Forest Model Started")
    result_df_2 = model_dict[date_str][actor_type][SKLEARN_ISOLATION_FOREST_MODEL].score(input_df_zsc)
    input_df_zsc["pas_isolation"] = result_df_2["PAS"].round(4)
    
    logger.info("Step 5:::::Scoring for One-Class SVM Model Started")
    result_df_3 = model_dict[date_str][actor_type][SKLEARN_ONECLASS_SVM_MODEL].score(input_df_zsc)
    input_df_zsc["pas_svm"] = result_df_3["PAS"].round(4)
    del input_df_zsc['PAS']

    logger.info("Step 6:::::Calculating overall Anomaly Score (PAS)")
    input_df_zsc["pas"] = input_df_zsc[["pas_kmeans", "pas_isolation", "pas_svm"]].mean(axis=1).round(4)

    logger.info("Step 7:::::Writing the results to database")
    sdf=spark.createDataFrame(input_df_zsc)

    logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.info("Step 6:::::Writing the results to database")
    write_spark_df_to_hdfs(spark=spark, output_schema=OUTPUT_SCHEMA , database = DATABASE, 
                           table_name = WRITE_TABLE_NAME,sdf=sdf, timestamp=timestamp)
    sdf.show(5,False)
    logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  



def train_and_save_profile_anomaly_model(spark, data_source, date_str, data_frame, data_path, categorical_colnames, numerical_colnames, actor_type, model_dict1, logger):
    logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")        
    logger.info("Step 1:::::Training Cluster Profile Model")   
    kmeans_model = ClusterProfileModel.train(spark=spark,
                                      categorical_colnames=categorical_colnames,
                                      numerical_colnames=numerical_colnames, data_path=data_path,
                                      sdf=data_frame,
                                      anomaly_type=PROFILE_ANOMALY_TYPE
                                     )
    kmeans_model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source, day=date_str, 
                                                   anomaly_type=PROFILE_ANOMALY_TYPE, 
                                                   actor_type=actor_type,
                                                   model_name=PYSPARK_KMEANS_MODEL)

    logger.info("Step 1a:::::Saving Cluster Profile Model")
    kmeans_model.save(path=kmeans_model_path)

    logger.info("Step 1b:::::Loading the model into a dictionary")
    model_dict1[date_str][actor_type][PYSPARK_KMEANS_MODEL] = ClusterProfileModel.load(spark=spark, path=kmeans_model_path)
    
    logger.info("Step 2:::::Training for Isolation Forest Model") 
    isolation_model = IsolationForestProfileModel.train(spark=spark,
                                            categorical_colnames=categorical_colnames,
                                            numerical_colnames=numerical_colnames, data_path=data_path,
                                            sdf=data_frame,
                                            anomaly_type=PROFILE_ANOMALY_TYPE)

    isolation_model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source, day=date_str, 
                                                   anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   actor_type=actor_type,
                                                   model_name=SKLEARN_ISOLATION_FOREST_MODEL)
    logger.info("Step 2a:::::Saving Isolation Forest Model") 
    isolation_model.save(spark=spark, path=isolation_model_path)

    logger.info("Step 2b:::::Loading the model into a dictionary")
    model_dict1[date_str][actor_type][SKLEARN_ISOLATION_FOREST_MODEL] =  IsolationForestProfileModel.load(spark=spark, path=isolation_model_path)
    
    logger.info("Step 3:::::Training for One-Class SVM Model") 
    svm_model = OneClassSVMProfileModel.train(spark=spark,
                                        categorical_colnames=categorical_colnames,
                                        numerical_colnames=numerical_colnames, data_path=data_path,
                                        sdf=data_frame,
                                        anomaly_type=PROFILE_ANOMALY_TYPE)

    svm_model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source, day=date_str, 
                                                   anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   actor_type=actor_type,
                                                   model_name=SKLEARN_ONECLASS_SVM_MODEL)

    logger.info("Step 3a:::::Saving One-Class SVM Model")
    svm_model.save(spark=spark, path=svm_model_path)   

    logger.info("Step 3b:::::Loading the model into a dictionary")
    model_dict1[date_str][actor_type][SKLEARN_ONECLASS_SVM_MODEL] = OneClassSVMProfileModel.load(spark=spark, path=svm_model_path)
   
    logger.info("Training and loading Completed for all 3 models")
    logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    return model_dict1
  

def main():
    data_source = DATA_SOURCE
    table_name = TABLE_NAME
    database = DATABASE
    categorical_colnames = PROFILE_MODEL_CATEGORICAL_COLUMNS
    numerical_colnames = PROFILE_MODEL_NUMERICAL_COLUMNS
    log_path = get_log_path(data_source, "profile_anomaly_train_and_score")
    logger = configure_logger(logger_name="profile_anomaly_train_and_score", log_path=log_path, log_level=LOG_LEVEL)
    app_name = data_source + "_profile_anomaly_train_and_score"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    data_store = HiveDataStore(spark=spark)
    no_of_days = int(NO_OF_DAYS)
    logger.info("###################################################################################")        
    logger.info("ML Model Process Started")
    model_dict1 = dict()
    for i in range(0, no_of_days):
        date_proc = datetime.datetime.strptime(DATE_ARGUMENT, "%Y-%m-%d")
        date_act = date_proc - timedelta(days=i)
        year = date_act.year
        month = date_act.month
        day = date_act.day
        date_str = date_act.strftime("%Y-%m-%d")
        model_dict1[date_str] = dict()
        logger.info("###################################################################################")
        logger.info("Reading the data for {day}".format(day=date_str))
        sdf_day = data_store.read_spark_df_from_win_data_store(spark=spark,  year=year, month=month, day=day, table_name=table_name, database=database)
        
        for actor in ACTOR_TYPE:
            model_dict1[date_str][actor] = dict()
            data_path = USER_PROFILE_DATA_PATH.format(data_source=data_source, day=date_str, actor_type=actor, anomaly_type=PROFILE_ANOMALY_TYPE)
            timestamp = int(time.time() * 1000)    
            if actor == "user":
                sdf_day_actor = sdf_day.filter("type='user'")
            else:
                sdf_day_actor = sdf_day.filter("type='entity'")               
                
            logger.info("###################################################################################")
            logger.info("Profile anomaly model training for {actor} started for {day}".format(actor=actor, day=date_str))
            
            model_dict1 = train_and_save_profile_anomaly_model(spark=spark, data_source=data_source, date_str=date_str, data_frame=sdf_day_actor, data_path=data_path,
                                                    categorical_colnames=categorical_colnames,
                                                    numerical_colnames=numerical_colnames, actor_type=actor, model_dict1=model_dict1, logger=logger)
                    
            logger.info("###################################################################################")
            logger.info("Profile anomaly model scoring for {actor} started for {day}".format(actor=actor, day=date_str))
            score_profile_anomaly_model(spark=spark, data_frame=sdf_day_actor,
                                        categorical_colnames=categorical_colnames,
                                        numerical_colnames=numerical_colnames,
                                        model_dict=model_dict1, actor_type=actor, date_str=date_str, 
                                        timestamp=timestamp, logger=logger)    
            logger.info("###################################################################################")
            logger.info("Profile anomaly model training and scoring for {day} for {actor} completed successfully!".format(day=date_str, actor=actor))                       
        day =   day - 1      
    logger.info("ML Model Process Completed")
    logger.info("###################################################################################") 

if __name__ == "__main__":
    main()

