from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


class MinMaxPipeline(object):
    def __init__(self, pipeline_model):
        self.pipeline_model = pipeline_model

    @classmethod
    def train(cls, df, colname):
        score = df[colname].values.reshape(-1, 1)
        minmax_scaler = MinMaxScaler(feature_range=(0, 100))
        scoring_pipeline = Pipeline(steps=[("MinMaxScaler", minmax_scaler)])
        scoring_pipeline_model = scoring_pipeline.fit(score)

        return MinMaxPipeline(pipeline_model=scoring_pipeline_model)
