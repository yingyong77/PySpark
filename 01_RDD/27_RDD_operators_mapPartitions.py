# -*- coding: utf-8 -*-
"""
性能要比map好很多
一次传递一整个分区的数据

map一次传递一个数据
大大减少了网络io

@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 4, 5, 7, 2], 3)

    def process(iter):
        result = list()
        for it in iter:
           result.append(it * 10)

        return result


    print(rdd.mapPartitions(process).collect())

