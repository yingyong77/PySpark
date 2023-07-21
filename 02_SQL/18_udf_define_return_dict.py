# -*- coding: utf-8 -*-
"""
字典返回类型的UDF

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

    # 有三个数字 1 2 3 传入数字 返回数字所在的序号相应的字母 然后和数字形成字典返回
    df = sc.parallelize([[1], [2], [3]]).toDF(['num'])


    def process(data):
        return {"num":data, "letters": string.ascii_letters[data]}
    # udf 返回值是字典 需要用StructType
    udf_dic = spark.udf.register("udf_dic", process, StructType()
                       .add("num", IntegerType(),nullable=True)
                       .add("letters", StringType(), nullable=True))

    df.select(udf_dic(df['num'])).show(truncate=False)

    df.createTempView("t_dic_transform")
    spark.sql("""
    select udf_dic(num) from t_dic_transform
    """).show()

    df.selectExpr("udf_dic(num)").show()
    df.withColumn('num', udf_dic(df['num']))






