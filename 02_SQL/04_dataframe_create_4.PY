# -*- coding: utf-8 -*-
"""
基于pandas的dataframe构建sparksql的dataframe

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

    pdf = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["darren", "hsoleil", "joker"],
        "age": [11, 11, 12]
    })
    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
