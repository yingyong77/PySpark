# -*- coding: utf-8 -*-
"""

需求1：用户搜索的关键词分析
需求2: 用户和关键词组合分析
需求3: 热门搜索时间段分析

@author: Darren
@time: 2023/7/13 10:25
@function:
"""
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import contect_jieba, filter_words_data, append_words, extract_and_word
from operator import add

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile("../../data/input/SogouQ.txt")

    # 对数据进行切分\t
    split_rdd = file_rdd.map(lambda x: x.split("\t"))

    # 缓存
    split_rdd.persist(StorageLevel.DISK_ONLY)

    # 用户搜索的关键词分析
    context_rdd = split_rdd.map(lambda x: x[2])

    #  对搜索的内存进行分析
    word_rdd = context_rdd.flatMap(contect_jieba)
    # 院校 帮 ->院校帮
    # 搏学 谷 ->搏学谷
    # 传智播 客-> 传智播客

    # 过滤没用的关键词
    filter_rdd = word_rdd.filter(filter_words_data)

    # 组合原先的关键词 返回(data,1)元组结构
    final_words_rdd = filter_rdd.map(append_words)

    # print(final_words_rdd.takeSample(True, 3))

    # 对单词进行分组聚合排序
    result1 = final_words_rdd.reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda k: k[1], ascending=False, numPartitions=1)\
        .take(5)
    print(f"需求1结果 {result1}")

    # 需求2: 用户和关键词组合分析
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    # 对用户的内容进行分词， 分词之后和用户id再次进行组合
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_and_word)
    result2 =  user_word_with_one_rdd.reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], ascending=False, numPartitions=1)\
        .take(5)
    print(f"需求2结果 {result2}")

    # 需求3: 热门搜索时间段分析
    time_rdd = split_rdd.map(lambda x: x[0])
    # 对时间进行处理 只保留小时
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    result3 = hour_with_one_rdd.reduceByKey(add)\
        .sortBy(lambda x: x[1], False, numPartitions=1)\
        .collect()
    print(f"需求3结果 {result3}")

    time.sleep(101000)


