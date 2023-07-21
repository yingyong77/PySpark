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

    rdd = sc.parallelize([('c', 3), ('F', 1), ('d', 2), ('b', 1), ('a', 4), ('e', 2), ('z', 1), ('E', 1)], 3)

    # 使用sortBy进行排序

    # 按照key
    """注意：如果要全局有序，排序分区数请设置为1"""
    # 第三个参数 对排序前对key进行处理
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda x: str(x).lower()).collect())


