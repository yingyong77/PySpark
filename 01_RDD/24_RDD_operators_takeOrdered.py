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

    rdd = sc.parallelize([1, 3, 4, 5, 1, 3, 5, 2, 7, 8, 5, 6])

    # 保留几个数据
    # 参数2 默认是倒序排序
    print(rdd.takeOrdered(3))
