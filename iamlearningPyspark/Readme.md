## spark is balabala
### pyspark快速入门
```
1.SparkConf(LoadDefaults = True)， 设置Spark的一些配置，例：conf. = SparkConf().setAppName("MyApplication")

2.SparkContext(conf = None)，  一个SparkContext表示一个到Spark集群的连接，可以用来创建RDD和广播， 例：sc = SparkContext(conf = conf)

3.parallelize(Data)， textFile(Path)， 创建一个RDD，例：rdd = sc.parallelize([1,2,3,4,5]) ，  rdd = textFile("/root/log/*")

4.map(func)， 对RDD做map操作， 例： rdd.map(lambda word : (word, 1))

5.reduceByKey(func)， 对RDD做reduce操作， 例： rdd.reduceByKey(lambda value1, value2 : value1 + value2)

6.filter(func)， 对RDD做筛选， 例：rdd.filter(lambda data : len(data) < 20)

7.saveAsTextFile(path)， RDD保存到硬盘， 例：rdd.saveAsTextFile("/root/log/test")

8.cache()，数据持久化， 例 rdd.cache()； unpersist()，数据删除，例：rdd.unpersist()

9.collect()：返回RDD的list   union(otherDataset)：合并数据集     join(otherDataset)：左连接  broadcast(value)：广播一个只读变量到集群，使用返回值

10.repartition(numPartitions)：重新分片    flatMap()：比map多一个扁平化处理   distinct()：去重
```

### This is a example about pyspark
```
#!/usr/bin/env python
# coding: utf8
# 1006793841@qq.com
#
from pyspark.sql import SparkSession
import pytispark.pytispark as pti
import json

spark = SparkSession.builder.master("spark://192.168.204.52:7077").appName("save_to_tidb").getOrCreate()
ti = pti.TiContext(spark)
ti.tidbMapDatabase("tpch_001")

##### this is a RDD list 
#f = spark.sql("SELECT * FROM `CUSTOMER` where C_CUSTKEY <4").collect()

##### this is a json format
f = spark.sql("SELECT C_CUSTKEY, C_NAME, C_ADDRESS FROM `CUSTOMER` WHERE C_CUSTKEY <40").toJSON().collect()

##### <class 'pyspark.sql.types.Row'>
#f = spark.sql("SELECT C_CUSTKEY, C_NAME, C_ADDRESS FROM `CUSTOMER` WHERE C_CUSTKEY <4").toDF('a','b','c').collect()

##### save to a csv file with header
#f = spark.sql("SELECT C_CUSTKEY, C_NAME, C_ADDRESS FROM `CUSTOMER` WHERE C_CUSTKEY <4").toDF('a','b','c').write.option("header",False).csv('jjj')

##### save to a csv file
#f = spark.sql("SELECT C_CUSTKEY, C_NAME, C_ADDRESS FROM `CUSTOMER` WHERE C_CUSTKEY <4").toDF('a','b','c').write.csv('bbb')

print f
print type(f)
print "*"*99
rows = []
for row in f:
  #print "this is line"i[C_CUSTKEY]
  print row
  print type(row)
  I = json.loads(row)
  print I
  print type(I)
  I = tuple(I.values()) # 字典是无序的集合，不能保证字段的有序，pass!!!
  rows.append(I)
  print "\033[31m ****************** next *********************\033[0m"
print "#"*30,"\033[32mthis is tuple rows\033[0m","#"*30
print rows

```
### this is a python script read data from TiDB and insert into mysql
```
#!/usr/bin/env python
# coding: utf8
# 1006793841@qq.com
#
from pyspark.sql import SparkSession
import pytispark.pytispark as pti

spark = SparkSession.builder.master("spark://192.168.204.52:7077").appName("save_to_tidb").getOrCreate()
ti = pti.TiContext(spark)
ti.tidbMapDatabase("tpch_001")

#spark.sql("SELECT * FROM `CUSTOMER` where C_CUSTKEY <10").show()
df = spark.sql("SELECT CURRENT_DATE as commit_date, sum(C_ACCTBAL) FROM `CUSTOMER`")

def write_tidb(df):
    df.write.mode("append").format("jdbc").options(
		url='jdbc:mysql://192.168.204.50:4000/test',
		useSSL=False,
		user='root',
		password='',
		dbtable="insert_test",   # 表名
		batchsize="100",
		isolationLevel= "NONE").save()

if __name__ == "__main__":
    write_tidb(df)

	
##
##
#  脚本需要在Spark集群节点上执行
#  
#  1，安装 Spark集群并修改相关配置文件conf/ slaves, spark-default.conf, spark-env.sh
#  2，pip install pytispark
#  3，下载tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar 和mysql-connector-java-5.1.44.jar
#  4，

```


## what will be the next ??? 
## see you tomorrow










