# -*- coding: utf-8 -*-
"""
wholeTextFile处理小文件的api
比textFile api的性能要好很多
@author: Darren
@time: 2023/7/11 15:05
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("../data/input/tiny_files")
    print(rdd.map(lambda x: x[1]).collect())
