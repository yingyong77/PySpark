# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/11 20:17
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    # 通过groupBy对数据进行分组
    # groupBy传入参数的意思是：通过这个函数，确定按照谁来分组
    # 分组规则 和sql是一致的 相同的在一个组
    result_rdd = rdd.groupBy(lambda t: t[0])
    print(result_rdd.map(lambda x: list(x[1])).collect())
    