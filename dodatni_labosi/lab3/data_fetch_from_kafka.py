# command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 data_fetch_from_kafka.py
# I also ran the following command to recognize python as a valid command: set PYSPARK_PYTHON=python
#but  decided to create <spark_dir>/conf\spark-defaults.conf with set:spark.pyspark.python=python
#all resources were deleted from Azure after the lab was finished, hence I won't have any csv files to show
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaRead") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("subscribe", "topic1") \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/tpiuo/dodatni_labosi/lab3") \
    .option("checkpointLocation", "/tpiuo/dodatni_labosi/lab3") \
    .start()

query.awaitTermination()
