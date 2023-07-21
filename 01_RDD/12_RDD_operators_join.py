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

    rdd1 = sc.parallelize([(1001, 'darren'), (1002, 'hsoleil'), (1003, 'spark'), (1004, 'python')])
    rdd2 = sc.parallelize([(1001, '研发部'), (1002, '财务部')])

    # 通过join算子来进行rdd直接的关联
    # 对于join算子来说 关联条件 根据二元元祖的key进行关联
    # 全连接
    rdd3 = rdd1.join(rdd2)
    print(rdd3.collect())

    # 左外连接
    print(rdd1.leftOuterJoin(rdd2).collect())

    # 右外连接
    print(rdd1.rightOuterJoin(rdd2).collect())


