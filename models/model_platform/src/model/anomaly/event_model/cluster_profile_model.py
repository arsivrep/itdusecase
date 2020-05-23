import numpy as np
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import lit
from pyspark.sql.types import *

from model_platform.src.model.abstract_model import AbstractModel
from model_platform.src.model.anomaly.base_model.pyspark_kmeans.kmeans_pipeline import KMeansPipeline
from model_platform.src.model.anomaly.base_model.pyspark_kmeans.kmeans_utils import udf_calculate_SED
from model_platform.src.model.anomaly.base_model.pyspark_kmeans.minmax_pipeline import MinMaxPipeline


class ClusterEventModel(AbstractModel):
    def __init__(self, kmeans_model, minmax_model):
        self.kmeans_model = kmeans_model
        self.minmax_model = minmax_model

    @classmethod
    def train(cls, spark, data_store, **args):
        categorical_colnames = args["cat_colnames"]
        numerical_colnames = args["num_colnames"]
        print("inside train")
        print(numerical_colnames)
        sdf = args["sdf"]
        event_sdf = sdf
        if sdf is None:
            event_sdf = data_store.read_spark_df_from_data_store(**args)

        kmean_preprocess_model = None
        kmean_postprocess_model = None

        if not event_sdf.rdd.isEmpty():
            kmean_preprocess_model = KMeansPipeline.train(spark=spark, sdf=event_sdf,
                                                          cat_colnames=categorical_colnames,
                                                          num_colnames=numerical_colnames)

            centroids = np.array(kmean_preprocess_model.pipeline_model.stages[-1].clusterCenters())

            result_cluster_sdf = kmean_preprocess_model.pipeline_model.transform(event_sdf).persist()

            result_score_sdf = result_cluster_sdf.withColumn("sed",
                                                             udf_calculate_SED(centroids)(col("features"))).persist()

            kmean_postprocess_model = MinMaxPipeline.train(spark=spark, result_score_sdf=result_score_sdf,
                                                           colnames=["sed"])

        return ClusterEventModel(kmeans_model=kmean_preprocess_model, minmax_model=kmean_postprocess_model)

    def save(self, path):
        if self.kmeans_model != None and self.minmax_model != None:
            self.kmeans_model.pipeline_model.write().overwrite().save(
                path + "/clustering_pipeline_model")

            self.minmax_model.pipeline_model.write().overwrite().save(
                path + "/scoring_pipeline_model")

    @classmethod
    def load(cls, spark, path):
        try:
            clustering_pipeline_model = PipelineModel.load(path + "/clustering_pipeline_model")
            kmeans_model = KMeansPipeline(pipeline_model=clustering_pipeline_model)
            scoring_pipeline_model = PipelineModel.load(path + "/scoring_pipeline_model")
            minmax_model = MinMaxPipeline(pipeline_model=scoring_pipeline_model)
        except:
            kmeans_model = None
            minmax_model = None
        return ClusterEventModel(kmeans_model=kmeans_model,
                                 minmax_model=minmax_model)

    def score(self, sdf):
        if sdf.rdd.isEmpty():
            return None
        if self.kmeans_model != None and self.minmax_model != None:
            result_cluster_sdf = self.kmeans_model.pipeline_model.transform(sdf)
            centroids = np.array(self.kmeans_model.pipeline_model.stages[-1].clusterCenters())
            result_sed_sdf = result_cluster_sdf.withColumn("sed", udf_calculate_SED(centroids)(col("features")))
            result_scaler_sdf = self.minmax_model.pipeline_model.transform(result_sed_sdf).persist()

            def format_anomaly_score(value):
                val = float(value[0]) * 100
                if val > 100:
                    return 100.0
                if val < 0:
                    return 0.0
                return val

            udf_format_anomaly_score = udf(format_anomaly_score, FloatType())

            result_score_sdf = result_scaler_sdf.withColumn("EAS", udf_format_anomaly_score("scaled_sed"))
        else:
            result_score_sdf = sdf.withColumn("EAS", lit(0.0))

        return result_score_sdf
