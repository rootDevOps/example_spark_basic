from pyspark.sql import SparkSession

scSpark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sdfData = scSpark.read.csv("__temp__/PO19990420.CSV", header=True, sep=",")
sdfData.show(3)

print(sdfData.rdd.getNumPartitions())

"""
Convert to JSON
"""
# sdfData.write.json("/tmp/json/zipcodes.json")
