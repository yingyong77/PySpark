# -*- coding: utf-8 -*-
"""
数据写出 text csv  json parquet
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
    # 1. 读取数据集
    schema = StructType().add("user_id", StringType(), nullable=True). \
        add("movie_id", IntegerType(), nullable=True). \
        add("rank", IntegerType(), nullable=True). \
        add("ts", StringType(), nullable=True)
    df = spark.read.format("csv"). \
        option("sep", "\t"). \
        option("header", False). \
        option("encoding", "utf-8"). \
        schema(schema=schema). \
        load("../data/input/sql/u.data")

    # write txt 写出 只能写出一个列的数据 需要将df转换为单列
    df.select(f.concat_ws("---", "user_id", "movie_id", "rank", "ts")) \
        .write \
        .mode("overwrite") \
        .format("text") \
        .save("../data/output/sql/text")

    # csv
    df.write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ";")\
        .option("header", True)\
        .save("../data/output/sql/csv")

    # json
    df.write \
        .mode("overwrite") \
        .format("json") \
        .save("../data/output/sql/json")

    # json
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save("../data/output/sql/parquet")
