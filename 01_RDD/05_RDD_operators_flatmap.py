# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/11 16:24
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["hadoop spark flink", "spark hadoop hadoop", "hadoop spark hadoop"])
    # 得到所有的单词 组成RDD
    # rdd2 = rdd.map(lambda x: x.split(" "))

    # 得到所有的单词, 组成RDD, flatMap
    rdd2 = rdd.flatMap(lambda x: x.split(" "))

    print(rdd2.collect())