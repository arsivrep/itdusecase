import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, OneHotEncoder


class IsolationForestPipeline(object):
    def __init__(self, pipeline_model):
        self.pipeline_model = pipeline_model

    @classmethod
    def train(cls, df, cat_colnames, num_colnames):
        if len(num_colnames) > 0:
            num_data = df[num_colnames].values.astype(np.float64)
        if len(cat_colnames) > 0:
            cat_data = df[cat_colnames].values

        standard_scaler = None
        if len(num_colnames) > 0:
            standard_scaler = StandardScaler()
            num_data_normalized = standard_scaler.fit_transform(num_data)

        one_hot_encoder = None
        if len(cat_colnames) > 0:
            one_hot_encoder = OneHotEncoder(categories='auto', handle_unknown="ignore")
            cat_data_encoded = one_hot_encoder.fit_transform(cat_data).toarray()

        if len(num_colnames) > 0 and len(cat_colnames) > 0:
            data = np.concatenate((num_data_normalized, cat_data_encoded), axis=1)
        elif len(cat_colnames):
            data = cat_data_encoded
        elif len(num_colnames):
            data = num_data_normalized

        isolation_forest = IsolationForest(behaviour='new', n_estimators=1000, max_samples="auto",
                                           max_features=min(2, len(num_colnames + cat_colnames)),
                                           bootstrap=True,
                                           contamination="auto",
                                           random_state=42)
        isolation_forest_model = isolation_forest.fit(data)

        isolation_forest_pipeline_model = {"cat_colnames": cat_colnames, "num_colnames": num_colnames,
                                           "standard_scaler": standard_scaler,
                                           "one_hot_encoder": one_hot_encoder,
                                           "isolation_forest_model": isolation_forest_model}
        return IsolationForestPipeline(pipeline_model=isolation_forest_pipeline_model)
