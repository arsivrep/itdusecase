import logging
import socket
import sys

import flask
from flask import Flask, request
from flask_cors import CORS
from pyspark.sql import SparkSession

from model_platform.src.operationalization.anomaly.profile_anomaly.online_score import \
    load_profile_anomaly_model, \
    get_profile_anomaly_score

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
app = Flask(__name__)
CORS(app)

spark = SparkSession.builder.appName('MAAS').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
model_dict = dict()
model_dict["windowsos"] = load_profile_anomaly_model(spark=spark, data_source="windowsos")
model_dict["wgtraffic"] = load_profile_anomaly_model(spark=spark, data_source="wgtraffic")
model_dict["msexchange"] = load_profile_anomaly_model(spark=spark, data_source="msexchange")


@app.route('/')
def heart_beat():
    return flask.jsonify({"status": "ok"})


@app.route('/apply', methods=['GET'])
def calculate_profile_outlier_score():
    input_json = request.args
    input_dict = dict(input_json)
    app.logger.info("input request : {input_dict}".format(input_dict=input_dict))
    data_source = input_dict["data_source"][0]
    if "src_ip" in input_dict:
        app.logger.info("calculating ip profile anomaly score for {data_source}".format(data_source=data_source))
        score = get_profile_anomaly_score(spark=spark, model_dict=model_dict[data_source],
                                          input_req=input_dict, entity_type="ip")
    elif "user_name" in input_dict:
        app.logger.info("calculating user profile anomaly score for {data_source}".format(data_source=data_source))
        score = get_profile_anomaly_score(spark=spark, model_dict=model_dict[data_source],
                                          input_req=input_dict, entity_type="user")
    else:
        score = None

    app.logger.info("response : {score}".format(score=score))
    return flask.jsonify(score)


if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    sock.close()
    with open("endpoint.dat", "w") as text_file:
        text_file.write("{\"url\" : \"http://0.0.0.0:%d\"}" % port)
    app.run(threaded=True, host="0.0.0.0", port=port)
