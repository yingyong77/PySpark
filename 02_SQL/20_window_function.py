# -*- coding: utf-8 -*-
"""
窗口函数

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

    rdd = sc.parallelize([('张三', 'class_1', 99),
                          ('王五', 'class_2', 35),
                          ('王三', 'class_3', 57),
                          ('王久', 'class_4', 12),
                          ('王丽', 'class_5', 99),
                          ('王娟', 'class_1', 90),
                          ('王军', 'class_2', 91),
                          ('王俊', 'class_3', 33),
                          ('王君', 'class_4', 55),
                          ('王珺', 'class_5', 66),
                          ('郑颖', 'class_1', 11),
                          ('郑辉', 'class_2', 33),
                          ('张丽', 'class_3', 36),
                          ('张张', 'class_4', 79),
                          ('黄凯', 'class_5', 90),
                          ('黄开', 'class_1', 90),
                          ('黄恺', 'class_2', 90),
                          ('王凯', 'class_3', 11),
                          ('王凯杰', 'class_1', 11),
                          ('王开杰', 'class_2', 3),
                          ('王景亮', 'class_3', 99)
                          ])

    schema = StructType().add("name", StringType()). \
        add("class", StringType()). \
        add("score", IntegerType())
    df = rdd.toDF(schema)

    df.createTempView("stu")
    # 聚合类型
    spark.sql("""
    select *,AVG(score) over() as avg_score from stu
    """).show()

    #  排序相关的窗口函数 RANK OVER 、DENSE_RANK over、ROW_NUMBER over
    #  RANK OVER
    #  DENSE_RANK  相同排名的属于同一等级 比如 排名1的有多个
    #  ROW_NUMBER  分配一个唯一的整数值
    spark.sql("""
    select *, ROW_NUMBER() over(order by score desc) as row_number_rank,
    DENSE_RANK() over(partition by class order by score desc) as dense_rank,
    RANK() over(order by score) as rank
    from stu
    """).show()

    #NTILE  均分成6份
    spark.sql("""
     select *, NTILE(6) OVER(order by score desc) from stu
    """).show()




