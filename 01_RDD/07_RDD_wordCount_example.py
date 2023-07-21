# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/11 18:27
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取文件获取数据构建rdd
    rdd = sc.textFile("../data/input/words.txt")
    word_rdd = rdd.flatMap(lambda x: x.split(" "))
    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a+b)

    print(result_rdd.collect())
