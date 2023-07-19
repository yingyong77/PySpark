# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/18 11:05
@function:
"""
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("test")\
        .master("local[*]")\
        .getOrCreate()
    # 基于RDD转换成dataFrame
    sc = spark.sparkContext
    rdd = sc.textFile("../data/input/sql/people.txt")\
        .map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[1])))
    # [[hsoleil, 1], [darren, 111]]

    # 构建dataframe对象 structType
    # StructField 可以自动识别
    # 参数1.被转换的rdd
    # 参数2 指定列名 通过list的形式指定，按照顺序依次提供字符串名称即可
    df = spark.createDataFrame(rdd, schema=['name', 'age'])

    # 打印dataframe的表结构
    df.printSchema()

    # 打印df中的数据
    # 参数2 表示是否对列进行截断，如果列的长度超过20的话 后续的长度不显示以...代替 FALSE表示不截断
    df.show(20, False)

    # 将df对象转换成临时表，可供sql语句查询
    df.createTempView("people")
    spark.sql("""
    select * from people where  age < 30
    """).show()