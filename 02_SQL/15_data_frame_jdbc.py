# -*- coding: utf-8 -*-
"""
jdbc
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

    # spark.sparkContext.setLogLevel("DEBUG")

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

    # df.show()

    # 写出数据到mysql数据库中
    # jdbc_url = 'jdbc:mysql://101.35.90.82:3307/darren?useSSL=FALSE'
    # df.write.mode("overwrite")\
    #     .format("jdbc")\
    #     .option("url",jdbc_url)\
    #     .option("dbtable", "movie_data")\
    #     .option("user", "root")\
    #     .option("password", "darren123")\
    #     .save()

    properties = {
        "user": "root",                    # MySQL 数据库的用户名
        "password": "darren"                 # MySQL 数据库的密码
    }
    jdbc_url2 = 'jdbc:mysql://101.35.90.82:3306/darren_db?useSSL=FALSE'

    df2 = spark.read.format("jdbc") \
        .option("url",jdbc_url2) \
        .option("dbtable", "tb_hotel") \
        .option("user", "root") \
        .option("password", "darren")\
        .load()

    df2.printSchema()
    df2.show()

    spark.stop()
"""
JDBC写出 会自动创建表
DataFrame中有表结构信息，StructType记录的 字段名称 类型 是否为空 
"""