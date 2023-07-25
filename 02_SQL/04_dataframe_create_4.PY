# -*- coding: utf-8 -*-
"""
基于pandas的dataframe构建spark sql的dataframe

koalas

@author: Darren
@time: 2023/7/18 11:05
@function:
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType ,IntegerType, StructField
import pandas as pd
import databricks.koalas as ks
import numpy as np


if __name__ == '__main__':

    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext

    pdf = pd.DataFrame({"id": [1, 2, 3],"name": ["darren", "hsoleil", "joker"],"age": [11, 11, 12]})
    # df = spark.createDataFrame(pdf)
    # df.printSchema()
    # df.show()

    data = {'name': ['Alice', 'Bob', 'Charlie', 'David', 'Emily'],
            'age': [25, 30, 35, 40, 45],
            'city': ['New York', 'San Francisco', 'Los Angeles', 'Chicago', 'Seattle']}

    df1 = spark.createDataFrame(pd.DataFrame(data))
    kdf = df1.to_koalas()

    # 使用 Koalas DataFrame 的 Pandas API 进行操作
    # 例如，计算年龄的平均值
    average_age = kdf['age'].mean()
    print("Average age:", average_age)

    # 筛选年龄大于等于 35 的行
    filtered_kdf = kdf[kdf['age'] >= 35]
    print("Filtered DataFrame:")
    print(filtered_kdf)
