# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/11 15:38
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    def add(data):
        return data * 10

    print(rdd.map(add).collect())

    # 更简单的方式是 定义lambda表达式来写匿名函数
    print(rdd.map(lambda data: data * 10).collect())



"""

对于算子的接收函数来说，两种方法都可以，
只不过lambda 适用于一行代码就搞定的函数体，如果是多行需要定义独立的方法
"""