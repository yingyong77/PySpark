# -*- coding: utf-8 -*-
"""
聚合加上初始值
分区内作用
分区间作用
@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 10), 3)
    rdd_fold = rdd.fold(10, lambda x, y: x + y)
    print(rdd_fold)