import numpy as np


def calculate_score(pipeline_model, df, output_colname):
    standard_scaler = pipeline_model["standard_scaler"]
    one_hot_encoder = pipeline_model["one_hot_encoder"]
    isolation_forest_model = pipeline_model["isolation_forest_model"]
    cat_colnames = pipeline_model["cat_colnames"]
    num_colnames = pipeline_model["num_colnames"]

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

    df[output_colname] = isolation_forest_model.decision_function(data).reshape(-1, 1) * -1
    return df
