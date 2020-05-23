from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler


class MinMaxPipeline(object):
    def __init__(self, pipeline_model):
        self.pipeline_model = pipeline_model

    @classmethod
    def train(cls, spark, result_score_sdf, colnames):
        assembler = VectorAssembler(inputCols=colnames, outputCol="sed_vec")
        scaler = MinMaxScaler(inputCol="sed_vec", outputCol="scaled_sed")

        scoring_pipeline = Pipeline(stages=[assembler, scaler])
        scoring_pipeline_model = scoring_pipeline.fit(result_score_sdf)

        return MinMaxPipeline(pipeline_model=scoring_pipeline_model)
