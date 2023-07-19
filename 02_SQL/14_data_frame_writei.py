# -*- coding: utf-8 -*-
"""
数据写出
@author: Darren
@time: 2023/7/18 11:05
@function:
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType ,IntegerType, StructField
from pyspark.sql import functions as f

if __name__ == '__main__':

    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", 2)\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext
    # 1.读取数据集

