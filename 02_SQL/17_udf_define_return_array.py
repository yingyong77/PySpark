# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/18 11:05
@function:
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, ArrayType
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext
    rdd = sc.parallelize([['Hadoop Spark Flink'], ['Hadoop Flink Java']])

    print(rdd.collect())
    df = rdd.toDF(['line'])

    df.show()

    def split_line(data):
        return data.split(' ')

    udf = spark.udf.register("udf", split_line, ArrayType(StringType()))

    df.select(udf(df['line'])).show(truncate=False)

    # SQL风格
    df.createTempView("lines")

    spark.sql(""" 
    select udf(line) from lines
    """).show(truncate=False)

    df.withColumn("line", udf(df["line"])).show(truncate=False)




