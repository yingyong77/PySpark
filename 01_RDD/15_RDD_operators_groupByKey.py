# -*- coding: utf-8 -*-
"""
对两个算子进行join操作(可实现SQL的内外连接)
join算子只能用于二元元组
@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])

    print(rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect())