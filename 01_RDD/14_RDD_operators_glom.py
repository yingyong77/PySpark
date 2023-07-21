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

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 4)
    print(rdd.glom().collect())
    # 解嵌套的操作
    print(rdd.glom().flatMap(lambda x: x).collect())


