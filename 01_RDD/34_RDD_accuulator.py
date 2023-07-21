 # -*- coding: utf-8 -*-
"""
代码运行是没问题的
但是会很占用内存

@author: Darren
@time: 2023/7/11 20:34
@function:
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    stu_list = {
        (1, "张大仙", 11),
        (2, "奶油", 13),
        (3, "可乐", 11),
        (4, "hsoleil", 11)
    }
    # 1.讲本地Python对象标记为广播变量
    broadcast = sc.broadcast(stu_list)

    score_info_rdd = sc.parallelize([
         (1, "语文", 99),
         (2, "数学", 99),
         (3, "英语", 99),
         (4, "编程", 99),
         (1, "语文", 99),
         (2, "编程", 99),
         (3, "语文", 99),
         (4, "英语", 99),
         (1, "语文", 99),
         (3, "英语", 99),
         (2, "编程", 99)
        ])


    def map_func(data):
        id = data[0]
        name = ""
        # 从使用到本地广播对象的地方，从广播变量中取出来用即可
        for stu_info in broadcast.value:
            info_id = stu_info[0]
            if info_id == id:
                name = stu_info[1]
        return name, data[1], data[2]


    print(score_info_rdd.map(map_func).collect())


"""
场景： 本地集合对象 python list和分布式集合对象 进行关联的时候
需要将本地集合对象封装成广播对象 可以减少
1.网络io的次数 
2.executor内存的占用
"""
