import os

import numpy as np

from config import *
from model_platform.src.model.abstract_model import AbstractModel
from model_platform.src.model.anomaly.base_model.sklearn_isolationforest.isolation_forest_pipeline import \
    IsolationForestPipeline
from model_platform.src.model.anomaly.base_model.sklearn_isolationforest.isolation_forest_utils import \
    calculate_score
from model_platform.src.model.anomaly.base_model.sklearn_isolationforest.minmax_pipeline import MinMaxPipeline


class IsolationForestProfileModel(AbstractModel):
    def __init__(self, iforest_model, minmax_model):
        self.iforest_model = iforest_model
        self.minmax_model = minmax_model

    @classmethod
    def train(cls, spark, **args):
        categorical_colnames = args["categorical_colnames"]
        numerical_colnames = args["numerical_colnames"]
        sdf = args["sdf"]
        entity_profile_sdf = sdf
        if sdf.count() > 0:
            entity_profile_df = entity_profile_sdf.toPandas()

            isolation_forest_pipeline_model = IsolationForestPipeline.train(df=entity_profile_df,
                                                                            cat_colnames=categorical_colnames,
                                                                            num_colnames=numerical_colnames)

            entity_profile_df = calculate_score(pipeline_model=isolation_forest_pipeline_model.pipeline_model,
                                                df=entity_profile_df,
                                                output_colname="score")

            scoring_pipeline_model = MinMaxPipeline.train(df=entity_profile_df, colname="score")

        else:
            isolation_forest_pipeline_model = None
            scoring_pipeline_model = None

       
        return IsolationForestProfileModel(iforest_model=isolation_forest_pipeline_model, minmax_model=scoring_pipeline_model)

    def save(self, spark, path):
        if self.iforest_model != None and self.minmax_model != None:
            os.system("hdfs dfs -rm -r " + path)
            os.system("rm -rf " + path)

            isolation_forest_pipeline_model_rdd = spark.sparkContext.parallelize([self.iforest_model])
            isolation_forest_pipeline_model_rdd.saveAsPickleFile(path + "/iforest_pipeline_model")

            scoring_pipeline_model_rdd = spark.sparkContext.parallelize([self.minmax_model])
            scoring_pipeline_model_rdd.saveAsPickleFile(path + "/scoring_pipeline_model")

    @classmethod
    def load(cls, spark, path):
        try:
            isolation_forest_pipeline_model = spark.sparkContext.pickleFile(path + "/iforest_pipeline_model").collect()[
                0]
            scoring_pipeline_model = spark.sparkContext.pickleFile(path + "/scoring_pipeline_model").collect()[0]
        except:
            isolation_forest_pipeline_model = None
            scoring_pipeline_model = None
        return IsolationForestProfileModel(
            iforest_model=isolation_forest_pipeline_model,
            minmax_model=scoring_pipeline_model)

    def score(self, df):
        if len(df) == 0:
            return None
        if self.iforest_model != None and self.minmax_model != None:
            standard_scaler = self.iforest_model.pipeline_model["standard_scaler"]
            one_hot_encoder = self.iforest_model.pipeline_model["one_hot_encoder"]
            isolation_forest_model = self.iforest_model.pipeline_model["isolation_forest_model"]
            cat_colnames = self.iforest_model.pipeline_model["cat_colnames"]
            num_colnames = self.iforest_model.pipeline_model["num_colnames"]

            if len(num_colnames) > 0:
                num_data = df[num_colnames].values.astype(np.float64)

            if len(cat_colnames) > 0:
                cat_data = df[cat_colnames].values

            if len(num_colnames) > 0:
                num_data_normalized = standard_scaler.transform(num_data)
            if len(cat_colnames) > 0:
                cat_data_encoded = one_hot_encoder.transform(cat_data).toarray()

            if len(num_colnames) > 0 and len(cat_colnames) > 0:
                data = np.concatenate((num_data_normalized, cat_data_encoded), axis=1)
            elif len(cat_colnames):
                data = cat_data_encoded
            elif len(num_colnames):
                data = num_data_normalized

            score = isolation_forest_model.decision_function(data).reshape(-1, 1) * -1
            pas = self.minmax_model.pipeline_model.transform(score)
            df["PAS"] = pas
            df["PAS"] = df["PAS"].map(lambda x: x if x < 100 else 100)
            df["PAS"] = df["PAS"].map(lambda x: x if x > 0 else 0.0)
        else:
            df["PAS"] = [0.0 for i in range(len(df))]
        return df
