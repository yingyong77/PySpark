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

    df.createTempView("score")
    df.createOrReplaceTempView("score_2")
    # 在使用的时候 需要在前面加global_temp.
    df.createGlobalTempView("score_3")

    spark.sql("""select subject,count(*) as cnt from score group by subject""").show()
    spark.sql("""select subject,count(*) as cnt from score_2 group by subject""").show()
    spark.sql("""select subject,count(*) as cnt from global_temp.score_3 group by subject""").show()