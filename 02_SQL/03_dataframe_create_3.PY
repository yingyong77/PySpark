# -*- coding: utf-8 -*-
"""
使用rdd的todf去创建dataFrame
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
    rdd = sc.textFile("../data/input/sql/people.txt")\
        .map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[1])))

    # toDF的方式构建df 列的类型推断
    df = rdd.toDF(["name", "age"])
    df.printSchema()
    df.show()

    structType = StructType()\
        .add(StructField("name", StringType(), False))\
        .add(StructField("age", IntegerType(), False))
    df2 = rdd.toDF(structType)
    df2.printSchema()
    df2.show()