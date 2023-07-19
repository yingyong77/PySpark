# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/18 11:05
@function:
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType ,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext
    rdd = sc.textFile("../data/input/sql/people.txt")\
        .map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[1])))

    # 构建表结构的描述对象 structType对象
    schema = StructType().add("name", StringType(), False)\
        .add("age", IntegerType(), False)
    # 基于StructType对象去构建RDD到DF的转换
    DF = spark.createDataFrame(rdd, schema)