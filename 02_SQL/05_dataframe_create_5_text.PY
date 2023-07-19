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

    schema = StructType().add(StructField(name="data", dataType=StringType(), nullable=False))

    df = spark.read.format("text").schema(schema)\
        .load("../data/input/sql/people.txt")

    df.printSchema()
    df.show()
