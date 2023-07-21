# -*- coding: utf-8 -*-
"""
并行化集合创建RDD

@author: Darren
@time: 2023/7/11 10:44
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 默认的分区数根据机器中cpu的核心数有关
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print("默认分区数是:", rdd.getNumPartitions())

    rdd = sc.parallelize([1, 2, 3], 3)
    print("分区数是:", rdd.getNumPartitions())
    # 将RDD(分布式对象) 每个分区的数据都发送到Driver中 形成一个python list对象
    print("rdd的内容是:", rdd.collect())


