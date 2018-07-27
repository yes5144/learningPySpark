#!/usr/bin/env python
# coding: utf8
# 1006793841@qq.com
#
# 链接：https://blog.csdn.net/u010159842/article/details/78664758

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext("local[2]","Kafka_wordCount")

# 处理时间间隔为2s
ssc = StreamingContext(sc,2)
zookeeper = "192.168.204.52:2181,192.168.204.53:2181,192.168.204.54:2181"

# 打开一个TCP socket 地址 和 端口号
topic = {"test3":0,"test4":1,"test5":2} # 要列举出分区
groupid = "test-consumer-group"

streaming_lines = KafkaUtils.createStream(ssc, zookeeper,groupid,topic)
lines1 = streaming_lines.map(lambda x: x[1])  # 注意 取tuple下的第二个即为接收的kafka流

# 对2s内收到的字符串进行分割
words = lines1.flatMap(lambda line: line.split(" "))

# 映射为（word,1) 元组
pairs = words.map(lambda word: (word, 1))
wordcounts = pairs.reduceByKey(lambda x,y: x+y)

# 输出文本，前缀 自动加日期
wordcounts.saveAsTextFiles("/tmp/kafka")

wordcounts.pprint()

# 启动spark streaming应用
ssc.start()

# 等待计算终止
ssc.awaitTermination()


