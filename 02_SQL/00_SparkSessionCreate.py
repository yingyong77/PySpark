# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/18 10:15
@function:
"""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建sparkSession执行入口对象
    spark = SparkSession.builder \
        .appName("tests") \
        .master("local[*]") \
        .getOrCreate()

    # 通过sparkSession对象获取sparkContext对象
    sc = spark.sparkContext
    df = spark.read.csv("../data/input/stu_score.txt", sep=',', header=False)
    df2= df.toDF("id", "name", "score")
    df2.printSchema()
    df2.show()

    df2.createTempView("score")

    # SQL风格
    spark.sql("""
    select * from score where name = '语文' LIMIT 5
    """).show()

    # DSL风格 Domain-Specific Language，领域特定语言
    df2.where("name = '语文'").limit(5).show()