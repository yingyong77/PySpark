# -*- coding: utf-8 -*-
"""
分区算子
自定义分区
@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1), ('flink', 1), ('hadoop', 1), ('spark', 1)])

    def process(key):
        if 'hadoop' == key or 'hello' == key:
            # 表示去0号分区
            return 0
        if 'spark' == key:
            # 表示去1号分区
            return 1
        return 2


    # 第二个参数 传入key 然后自定义分区规则
    print(rdd.partitionBy(3, process).glom().collect())

