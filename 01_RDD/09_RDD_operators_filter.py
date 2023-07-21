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

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
    rdd_filter = rdd.filter(lambda x: x % 2 == 1)
    print(rdd_filter.collect())
