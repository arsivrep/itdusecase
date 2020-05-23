from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer


class KMeansPipeline(object):
    def __init__(self, pipeline_model):
        self.pipeline_model = pipeline_model

    @classmethod
    def train(cls, spark, sdf, cat_colnames, num_colnames):
        string_indexer_list = list()
        for cat_colname in cat_colnames:
            string_indexer = StringIndexer(inputCol=cat_colname, outputCol=cat_colname + "_index",handleInvalid="skip")
            string_indexer_list.append(string_indexer)

        out = []
        pipe = []
        if len(num_colnames) > 0:

            assembler = VectorAssembler(inputCols=num_colnames, outputCol="features_vec")
            standard_scaler = StandardScaler(inputCol="features_vec", outputCol="features_zs", withMean=True, withStd=True)
            out = [standard_scaler.getOutputCol()]
            pipe = [assembler, standard_scaler]
        assembler_2 = VectorAssembler(inputCols=[x.getOutputCol() for x in string_indexer_list] + out, outputCol="features")
        estimator = KMeans(featuresCol="features", predictionCol="cluster_id", k=4)

        clustering_pipeline = Pipeline(stages=string_indexer_list + pipe + [assembler_2] + [estimator])
        clustering_pipeline_model = clustering_pipeline.fit(sdf)

        return KMeansPipeline(pipeline_model=clustering_pipeline_model)
