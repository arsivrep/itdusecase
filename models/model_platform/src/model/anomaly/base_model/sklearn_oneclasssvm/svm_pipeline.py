import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.svm import OneClassSVM


class SVMPipeline(object):
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

        svm = OneClassSVM(kernel="rbf", gamma=0.1)
        svm_model = svm.fit(data)

        svm_pipeline_model = {"cat_colnames": cat_colnames, "num_colnames": num_colnames,
                              "standard_scaler": standard_scaler,
                              "one_hot_encoder": one_hot_encoder, "svm_model": svm_model}
        return SVMPipeline(pipeline_model=svm_pipeline_model)
