# -*- coding: utf-8 -*-
"""
数据清洗
@author: Darren
@time: 2023/7/18 11:05
@function:
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType ,IntegerType, StructField
from pyspark.sql import functions as f

if __name__ == '__main__':

    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", 2)\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext
    df = spark.read.format("csv").option("sep",";").option("header",True).load("../data/input/sql/people.csv")
    # 数据去重 无参数 对全部的列联合起来进行比较 去除重复值 只保留一条
    df.dropDuplicates(['age', 'job']).show()
    # 缺失值处理 nullable 对缺失值进行删除
    # 无参: 有空值就进行删除
    df.dropna().show()
    # thresh=3 至少满足三个有效列 否则就删除
    df.dropna(thresh=3).show()
    # 至少两列满足 且这两列为name ,age
    df.dropna(thresh=2, subset=['name', 'age']).show()
    # 缺失值处理也可以完成对缺失值进行填充
    # DataFrame的fillna 对缺失的列进行填充  全局填充
    df.fillna("loss").show()
    df.fillna(subset='job',value='N/A').show()
    # 通过一个字典对所有的列设定填充规则
    df.fillna({'name':"未知姓名","age": 1, "job": "worker"}).show()

