import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import *


def calculate_SED_from_all_centroids(value, centroids):
    single_point = value
    points = centroids

    dist = (points - single_point) ** 2
    dist = np.sum(dist, axis=1)
    dist = np.sqrt(dist)
    dist = np.sum(dist)
    return float(dist)


def udf_calculate_SED(centroid_list):
    return udf(lambda l: calculate_SED_from_all_centroids(l, centroid_list), FloatType())
