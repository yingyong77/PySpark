# -*- coding: utf-8 -*-
"""

@author: Darren
@time: 2023/7/18 11:05
@function:
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType ,IntegerType, StructField
import pandas as pd

if __name__ == '__main__':

    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext

    df = spark.read.format("csv") \
        .option("sep", ";") \
        .option("header", True) \
        .option("encoding", "utf-8") \
        .schema("name STRING, age INT, job STRING") \
        .load("../data/input/sql/people.csv")

    df.printSchema()
    df.show()
