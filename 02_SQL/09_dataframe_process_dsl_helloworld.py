# -*- coding: utf-8 -*-
"""

@author: Darren
@time: 2023/7/18 11:05
@function:
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType ,IntegerType, StructField

if __name__ == '__main__':

    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext

    # 这里不写option的原因是默认sep是逗号
    df = spark.read.format("csv") \
        .option("sep", ",") \
        .schema("id INT, subject STRING, score INT")\
        .load("../data/input/sql/stu_score.txt")

    # column对象的获取
    id_column = df["id"]
    subject_column = df["subject"]

    # DSL风格演示
    df.select(["id", "subject"]).show()
    df.select("id", "subject").show()
    df.select([id_column, subject_column]).show()

    # filter API
    df.filter("score < 99").show()
    df.filter(df["score"] < 99).show()

    # where api
    df.where("score < 99").show()
    df.where(df["score"] < 99).show()

    # groupby 分组聚合
    df.groupBy("subject").count().show()
    df.groupBy(df['subject']).count().show()

    r = df.groupBy("subject")
    # groupBy的返回值是一个有分组关系的数据结构，中转对象 有一些api供我们分组做聚合