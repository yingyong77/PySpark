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

    # withReplacement为true 表示可以取重复的元素 重复表示取重复位置的数据
    # 种子默认是随机
    print(rdd.takeSample(False, 5, 1))