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


    rdd = sc.parallelize([1, 3, 4, 5, 1, 3, 5, 2, 7, 8, 5, 6],3)
    rdd.saveAsTextFile("hdfs://192.168.98.130:9000/output/out1")

    """
    rdd有几个分区 生成的文件就有几份
    """

