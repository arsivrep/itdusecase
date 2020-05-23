import argparse

from pyspark.sql import SparkSession


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--record_count", required=True)
    ap.add_argument("--path", required=True)

    args = vars(ap.parse_args())
    database = args["database"]
    table = args["table"]
    path = args["path"]
    record_count = args["record_count"]

    print(database)
    print(table)
    print(record_count)
    print(path)

    app_name = "download_hive_table"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sdf = spark.sql("select * from " + database + "." + table + " limit " + record_count).persist()
    # sdf = spark.sql("select * from " + database + "." + table + " where y=2018 and m=6 and d=12 limit " + record_count).persist()
    sdf.show(10)
    sdf.repartition(1).write.json(path=path, mode="overwrite")


if __name__ == "__main__":
    main()
