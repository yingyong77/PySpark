# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType

"""
需求1.各个省份销售额的统计
需求2.Top3的销售省份中，有多少店铺达到日销售额1000+
需求3.Top3省份中，各省的平均单单价
需求4.Top3省份中，各个省份的支付类型比例

receivable:订单金额
storeProvince：销售省份
dateTS:订单的销售日期
payType:支付类型
storeId 店铺id

@author: Darren
@time: 2023/7/24 18:11
"""
if __name__ == '__main__':
    # .config("spark.sql.warehouses.dir","hdfs://node1:8082/user/hive/warehouse")\
    # .config("hive.metastore.uris","thrift://node3:9083")\

    spark = SparkSession.builder \
        .appName("sparkSqlExample") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.format("json").load("../../data/input/mini.json") \
        .dropna(thresh=1, subset=['storeProvince']) \
        .filter("storeProvince != 'null'") \
        .filter("receivable < 10000") \
        .select("storeProvince", "storeId", "receivable", "dateTS", "payType")

    # 各个省份销售额的统计
    province_sale_df = df.groupby("storeProvince").sum("receivable"). \
        withColumnRenamed("sum(receivable)", "money"). \
        withColumn("money", f.round("money", 2)) \
        .orderBy("money", ascending=False)

    # jdbc_url = 'jdbc:mysql://101.35.90.82:3306/bigData?useSSL=FALSE&characterEncoding=utf8'
    # province_sale_df.write.mode("overwrite")\
    #     .format("jdbc")\
    #     .option("url",jdbc_url)\
    #     .option("dbtable", "province_sale")\
    #     .option("user", "root")\
    #     .option("password", "darren")\
    #     .option("encoding", "utf-8")\
    #     .save()
    #
    top3_province = province_sale_df.limit(3).select("storeProvince")\
        .withColumnRenamed("storeProvince", "top3_storeProvince") \

    # 和原始的df进行内关联 数据关联后，就是全部都是top3省份的销售数据了
    # 湖南省 1 2023-07-25
    # from_unixTime 的精度是秒级 要对数据进行裁剪
    # Top3的销售省份中，有多少店铺达到日销售额1000+
    top3_province_join_df =  df.join(top3_province, on=df['storeProvince']==top3_province['top3_storeProvince'])
    # 缓存
    top3_province_join_df.persist(StorageLevel.MEMORY_AND_DISK)

    province_hot_store_count = top3_province_join_df.groupby("storeProvince", "storeId", f.from_unixtime(df["dateTS"].substr(0,10), "yyyy-MM-dd").alias("day")) \
           .sum("receivable")\
           .withColumnRenamed("sum(receivable)", "money")\
           .filter("money > 1000")\
           .dropDuplicates(['storeId'])\
           .groupBy("storeProvince")\
           .count()

    jdbc_url = 'jdbc:mysql://101.35.90.82:3306/bigData?useSSL=FALSE&characterEncoding=utf8'
    province_hot_store_count.write.mode("overwrite")\
        .format("jdbc")\
        .option("url",jdbc_url)\
        .option("dbtable", "province_hot_store_count")\
        .option("user", "root")\
        .option("password", "darren")\
        .option("encoding", "utf-8")\
        .save()

    #3. Top3省份中，各省的平均单单价
    top3_province_order_avg_df = top3_province_join_df.groupby("storeProvince").avg("receivable")\
    .withColumn("money", f.round("avg(receivable)",2))\
        .orderBy("money", ascending=False)

    jdbc_url = 'jdbc:mysql://101.35.90.82:3306/bigData?useSSL=FALSE&characterEncoding=utf8'
    top3_province_order_avg_df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url",jdbc_url) \
    .option("dbtable", "top3_province_order_avg") \
    .option("user", "root") \
    .option("password", "darren") \
    .option("encoding", "utf-8") \
    .save()

    # Top3省份中，各个省份的支付类型比例
    # 湖南省 支付宝 33%
    # 湖南省 微信 20%
    top3_province_join_df.createTempView("province_pay")

    # 每一条普通的数据之后都带上了统计信息
    # spark.sql("""
    #  select storeProvince,payType,count(1) OVER(partition by storeProvince) as total from province_pay
    # """).show()

    def udf_func(percent):
        return str(round(percent * 100)) + "%"

    my_udf = f.udf(udf_func, StringType())

    pay_type_df = spark.sql("""
    select storeProvince,payType, (count(payType)/total) as percent from 
     (
     select storeProvince,payType,count(1) OVER(partition by storeProvince) as total from province_pay
     )  as sub GROUP by storeProvince,payType,total
    """).withColumn("percent", my_udf("percent"))

    pay_type_df.show()

    pay_type_df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url","jdbc:mysql://101.35.90.82:3306/bigData?useSSL=FALSE&characterEncoding=utf8") \
        .option("dbtable", "pay_type") \
        .option("user", "root") \
        .option("password", "darren") \
        .option("encoding", "utf-8") \
        .save()

    top3_province_join_df.unpersist()
