import argparse

from pyspark.sql import SparkSession


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)

    args = vars(ap.parse_args())
    query = args["query"]

    print("query : ", query)

    app_name = "query_hive_table"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sdf = spark.sql(query)
    sdf.show(10)


if __name__ == "__main__":
    main()
