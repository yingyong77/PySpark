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

    rdd1 = sc.parallelize([1, 1, 3, 3])
    rdd2 = sc.parallelize(['a', 'b', 'c'])
    print(rdd1.union(rdd2).collect())

"""
union算子是不会去重的
RDD的类型不同也是可以合并的

"""
