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

    rdd = sc.parallelize([('c', 3), ('F', 1), ('d', 2), ('b', 1), ('a', 4), ('e', 2), ('z', 1), ('E', 1)], 3)
    print(rdd.sortBy(lambda x: x[1], ascending=False, numPartitions=1).collect())
