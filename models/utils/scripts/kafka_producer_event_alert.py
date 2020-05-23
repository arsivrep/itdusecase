from kafka import KafkaProducer
import sys
import time
import random
from random import randint
import json


def dict_to_binary(the_dict):
    d = json.dumps(the_dict)
    binary = str.encode(d)
    return binary


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


bootstrap_servers = 'localhost:9092'
topic = "test2"

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

for i in range(430, 440):
    time.sleep(0.5)
    src_ip = random.choice(["2.3.4.5", "3.6.5.4", "33.55.77.99"])
    is_alert = random.choice(["true", "false"])
    alerts = "is_nu=false,is_ne=false,is_uatone=false,is_uafromne=true,is_nwpu=true,is_nwpp=true,is_nactpu=false,is_nactpp=false,is_nacnpu=true,is_nacnpp=true"

    if is_alert == "true":
        my_dict = {'src_ip': src_ip,
                   "source_type": "wgtraffic",
                   "timestamp": int(time.time() * 1000000),
                   "guid": random_with_N_digits(10),
                   "wgnum1": i,
                   "wgnum2": i,
                   "wgcat1": random.choice(["True", "False"]),
                   "wgcat2": random.choice(["True", "False"]),
                   "is_alert": "true",
                   "alerts": alerts,
                   "y": random.choice(["2018", "2019", "2020"]),
                   "m": random.choice(list(range(1, 4))),
                   "d": random.choice(list(range(1, 6)))
                   }
    else:
        my_dict = {'src_ip': src_ip,
                   "source_type": "wgtraffic",
                   "timestamp": int(time.time() * 1000000),
                   "guid": random_with_N_digits(10),
                   "wgnum1": i,
                   "wgnum2": i,
                   "wgcat1": random.choice(["True", "False"]),
                   "wgcat2": random.choice(["True", "False"]),
                   "y": random.choice(["2018", "2019", "2020"]),
                   "m": random.choice(list(range(1, 4))),
                   "d": random.choice(list(range(1, 6)))
                   }

    value = dict_to_binary(my_dict)
    producer.send(topic=topic, value=value)


if __name__=="__main__":



    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = vars(ap.parse_args())
    params = json.loads(args["params"])
    data_source = params["data_source"]
    categorical_colnames = list(map(str, params["categorical_colnames"].strip('[]').split(',')))
    numerical_colnames = list(map(str, params["numerical_colnames"].strip('[]').split(',')))

    log_path = get_log_path(data_source, "profile_anomaly_train")
    logger = configure_logger(logger_name="profile_anomaly_train", log_path=log_path, log_level=LOG_LEVEL)

    logger.info(data_source)
    logger.info(categorical_colnames)
    logger.info(numerical_colnames)


