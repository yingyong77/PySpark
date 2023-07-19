# coding:utf8
# import os
# os.environ['PYSPARK_PYTHON'] = "~/anaconda3/envs/pyspark/bin/python"

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # Driver运行
    spark_conf = SparkConf().setMaster("local[*]").setAppName("WorldCountHelloWorld")
    # 通过spark_conf对象构建sparkContext对象
    spark_context = SparkContext(conf=spark_conf)

    # Executor运行
    # 需求: wordcount单词计数，读取HDFS上的words.txt文件，对其内部出现的单词数量进行统计
    # 读取hdfs文件
    # 分布式spark环境不能是本地路径 必须是hdfs上的文件或是网络上的地址 因为有节点可能找不到路径
    file = spark_context.textFile("hdfs://192.168.98.130:9000/test.txt")

    # 读取本地路径文件
    # file = spark_context.textFile("../data/input/words.txt")

    # 将单词进行切割，得到一个存储全部单词对象的集合
    words_rdd = file.flatMap(lambda line: line.split(" "))

    # 将单词转换为元组对象，key是单词，value是数字1
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元组的value进行分组 按照key进行分组 对所有的value进行执行聚合操作
    result_add = words_with_one_rdd.reduceByKey(lambda a, b: a+b)
    #  通过collect方法进行收集RDD的数据进行打印输出结果

    # Driver运行
    print(result_add.collect())

