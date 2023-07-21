 # -*- coding: utf-8 -*-
"""
累加器

分区发送的是数据 而不是内存指针 
因为driver跟executor可能不在同一个机器上  所以也不会给内存地址
各个机器的内存是隔离的

使用累加器变量

每个分区都累加到5
最终会统计到10

@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    # python本地变量
    # count = 0
    acmlt = sc.accumulator(0)

    def map_func(data):
        global acmlt
        acmlt += 1
        print(acmlt)

    #
    rdd2 = rdd.map(map_func)
    # 先缓存在执行
    rdd2.cache()
    rdd2.collect()

    rdd3 = rdd2.map(lambda x: x)
    rdd3.collect()

    # 1.分布式计算的累加问题 结果是0
    # 2.会得到20 因为rdd失效了 会重新构建
    # 为防止重新构建 加cache则可避免重新构建
    print(acmlt)




