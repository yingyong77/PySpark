# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 2, 2, 2, 3, 3, 3])
    rdd_distinct = rdd.distinct()
    print(rdd_distinct.collect())

    rdd2 = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('a', 2)])
    print(rdd2.distinct().collect())
