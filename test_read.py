import os
import re
from operator import add
from pyspark import SparkConf, SparkContext, SQLContext

datafile_csv = "__temp__/PO19990420.CSV"


def get_keyval(row):
    text = row.text
    text = re.sub("\\W", " ", text)
    words = text.lower().split(" ")
    return [[w, 1] for w in words]


def get_counts(df):
    df.show(2, False)
    mapped_rdd = df.rdd.flatMap(lambda row: get_keyval(row))
    counts_rdd = mapped_rdd.reduceByKey(add)
    word_count = counts_rdd.collect()



def process_csv(abspath, sparkcontext):
    sqlContext = SQLContext(sparkcontext)
    df = sqlContext.read.load(os.path.join(abspath, datafile_csv),
                              format='com.databricks.spark.csv',
                              header='true',
                              inferSchema='true')
    get_counts(df)


if __name__ == "__main__":
    abspath = os.path.abspath(os.path.dirname(__file__))
    conf = (SparkConf()
            .setMaster("local[20]")
            .setAppName("sample app for reading files")
            .set("spark.executor.memory", "2g"))
    sc = SparkContext(conf=conf)
    process_csv(abspath, sc)
