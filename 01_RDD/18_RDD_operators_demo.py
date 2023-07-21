# -*- coding: utf-8 -*-
"""
读取data文件夹中的order文件，提取北京的数据，组合北京和商品类别进行输出，同时对结果进行去重，得到北京售卖的商品类别信息
@author: Darren
@time: 2023/7/11 20:34
@function:
"""
import json

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    rdd = sc.textFile("../data/input/order.text")
    json_rdd = rdd.flatMap(lambda line: line.split("|"))
    # 通过python内置的json库,完成python字符串对字典的抓换
    dict_rdd = json_rdd.map(lambda json_str: json.loads(json_str))
    # print(json_rdd.map(lambda json_str: json.loads(json_str)).collect())

    # 过滤北京的数据
    dict_rdd_beiJin = dict_rdd.filter(lambda x: x['areaName'] == '北京')
    # print(dict_rdd_beiJin.collect())

    # 组合北京和商品类型 组成新的rdd
    dict_rdd_beiJin_category_rdd = dict_rdd_beiJin.map(lambda x: x['areaName']+"_"+x['category'])

    # 去重
    result_rdd = dict_rdd_beiJin_category_rdd.distinct()

    print(result_rdd.collect())


