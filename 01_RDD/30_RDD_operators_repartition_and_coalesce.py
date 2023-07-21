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

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1), ('flink', 1), ('hadoop', 1), ('spark', 1)], 3)

    print(rdd.repartition(1).getNumPartitions())

    print(rdd.repartition(5).getNumPartitions())

    # coalesce
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(1, shuffle=True).getNumPartitions())

