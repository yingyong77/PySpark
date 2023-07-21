# -*- coding: utf-8 -*-
"""
collect算子
将各个分区中的内容中的数据，统一集中到driver中，形成一个list对象
@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/input/words.txt")
    rdd2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))

    # 通过countByKey对key进行计数 Action算子 Action算子返回不是rdd
    result = rdd2.countByKey()
    # 返回值是字典
    print(result, type(result))
