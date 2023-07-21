# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/18 11:05
@function:
"""
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
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7]).map(lambda x:[x])
    df = rdd.toDF(["num"])

    # SparkSession.udf.register()
    def num_ride_10(num):
        return num * 10

    # 仅可以用于sql风格
    # 必须声明返回值类型 必须真实类型必须一致
    # 返回的udf2对象 仅可以用于DSL语法
    udf2 = spark.udf.register("udf1", num_ride_10, IntegerType())

    # SQL风格
    df.selectExpr("""udf1(num)""").show()
    # DSL风格
    df.select(udf2(df['num'])).show()

    # 仅能用于DSL风格
    udf3 = f.udf(num_ride_10, IntegerType())
    df.select(udf3(df['num'])).show()
