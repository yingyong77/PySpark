# -*- coding: utf-8 -*-
"""
udaf
传入多个值
返回一个值

@author: Darren
@time: 2023/7/18 11:05
@function:
"""
import string
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext

    rdd = sc.parallelize([1,2,3,4,5], 3)
    df = rdd.map(lambda x:[x]).toDF(['num'])

    # 用mapPartitions
    single_partition_rdd = df.rdd.repartition(1)

    def process(data):
        sum = 0
        for row in data:
            sum += row['num']
        return [sum]

    print(single_partition_rdd.mapPartitions(process).collect())






