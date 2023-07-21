# -*- coding: utf-8 -*-
"""
读取文件创建rdd
@author: Darren
@time: 2023/7/11 11:06
@function:
"""
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 通过textFile api 读取本地数据
    file_rdd1 = sc.textFile("../data/input/words.txt")
    print("file_rdd1的默认分区数为:", file_rdd1.getNumPartitions())
    print("file_rdd1内容是:", file_rdd1.collect())

    # 最小分区数参数的测试 最小分区数是参考值  spark有自己的判断给的太大 spark不会理会
    file_rdd2 = sc.textFile("../data/input/words.txt", 1)
    file_rdd3 = sc.textFile("../data/input/words.txt", 100)
    print("file_rdd1的默认分区数为:", file_rdd2.getNumPartitions())
    print("file_rdd1的默认分区数为:", file_rdd3.getNumPartitions())  #90

    # 通过textFile api 读取hdfs的数据测试
    hdfs_rdd = sc.textFile("hdfs://192.168.98.130:9000/test.txt")
    print("file_rdd1内容是:", hdfs_rdd.collect())






