 # -*- coding: utf-8 -*-
"""
正常的单词进行单词计数
特殊字符统计出现多少个

@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    file_rdd = sc.textFile("../data/input/accumulator_broadcast_data.txt")

    # 特殊字符定义
    abnormal_char = [",", ".", "!", "#", "$", "%"]
    # 将特殊字符list包装成广播变量
    broadcast = sc.broadcast(abnormal_char)

    # 对特殊字符次数做累加
    acmlt = sc.accumulator(0)

    # 数据处理 在python中有内容就是True, None就是False 去除空行
    lines_rdd = file_rdd.filter(lambda x: x.strip())

    # 去除前后空格
    data_rdd = lines_rdd.map(lambda line: line.strip())

    # 对数据进行切分 按照正则表达式切分  因为空格分隔符某些单词之间是两个或多个空格符
    # 正则表达式 空格但不知道是多少个空格
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))

    # 当前rdd中有正常单词 也有特殊字符的单词
    # 过滤数据 保留正常单词用于单词计数，在过滤的过程中  对特殊字符进行计数

    def filter_dunc(data):
        global acmlt
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            acmlt += 1
            return False
        return True

    normal_rdd = words_rdd.filter(filter_dunc)
    result_rdd = normal_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

    print(f"正常单词计数结果:{result_rdd.collect()}")
    print(f"特殊字符数量:{acmlt}")







