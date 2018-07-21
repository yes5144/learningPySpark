package bigdata.sql

import java.util.Properties

import bigdata.utils.SparkTools
//import org.apache.avro.generic.GenericData.StringType
//import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object sql_test extends SparkTools{
  val spark = buildSparkSession("spark_sql_study")

  def saveCsv(): Unit = {
    import spark.sqlContext.implicits._
    spark.sparkContext.makeRDD(1 to 1000000).map(row=>(row,row*row)).toDF("side","area").write.option("header",true).csv("hdfs://master:8082//datas/csv")
  }

  def saveJson(): Unit = {
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(1 to 100000).map(row =>(row,row*row)).toDF("sidej","areaj").write.json("hdfs://192.168.204.52:8082//datas/json")
  }

  def saveParquet: Unit = {
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(1 to 1000000).map(row =>(row,row*row)).toDF("side","area").write.parquet("hdfs://master:8082/datas/parquet")
  }

  def saveHDFS: Unit ={
    import spark.sqlContext.implicits._
    // text 仅支持1 volume，所以需要拼接
    spark.sparkContext.makeRDD(1 to 10000).map(row=>(row+" : "+row*row)).toDF("side : area").write.text("hdfs://master:8082/datas/text")
  }

  def saveMYSQL: Unit ={
    import spark.sqlContext.implicits._
    spark.sparkContext.makeRDD(1 to 1000).map(row=>(row,row*row)).toDF("side","area").write.jdbc("jdbc:mysql://47.52.117.69:3306/spark211_text","df",getMysqlProperty)
  }

  def getMysqlProperty: Properties = {
    val prop = new Properties()
    prop.setProperty("user","spark211_text")
    prop.setProperty("password","channel2@@")
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    prop
  }

  def readCSV: Unit ={
    import spark.sqlContext._
    val df1 = spark.read.option("header",true).csv("hdfs://master:8082//datas/csv").select("side","area")
    df1.printSchema()
//    df1.collect().foreach(row=>println(row.getInt(0)+"-"+row.getInt(1)))
    df1.collect().foreach(row=>println(row.getString(0)+"-"+row.getString(1)))
    df1.createTempView("t")
    sql("select * from t where side < 50").collect().foreach(row=>println(row.getString(0)+"::"+row.getString(1)))
  }

  def readJSON: Unit = {
    import spark.sqlContext._
    val df1 = spark.read.json("hdfs://192.168.204.52:8082//datas/json")
    df1.printSchema()
    df1.createTempView("t")
//    sql("select * from t where areaj < 200").collect().foreach(row=>println(row.getString(0)+"-"+row.getString(1)))
    sql("select * from t where areaj < 200").collect().foreach(row=>println(row.getLong(0)+":"+row.getLong(1)))
  }

  def readRDD: Unit ={
    import spark.sqlContext._
    import spark.sqlContext.implicits._
//    使用了二维的数组，也可以使用case class
    val df1 = spark.sparkContext.makeRDD(1 to 100).map(row=>(row,row*row)).toDF("side","area")
    df1.printSchema()
    df1.createTempView("t")
    sql("select * from t where area > 100 and area < 1000").collect().foreach(row=>println(row.getInt(0)+"-"+row.getInt(1)))

  }

  def readText:Unit={
    import spark.sqlContext._
    import spark.sqlContext.implicits._
    // 创建rdd
    val rdd = spark.sparkContext.textFile("hdfs://master:8082/datas/text")
    val rdd1 = rdd.map(row=>{
      val fields = row.split(":")
      Row(fields{0},fields{1})
    })
    // 创建scheme
    val tem = "side,area"
    val fileds = tem.split(",").map(filed=>StructField(filed,StringType,nullable = true))
    val schema = StructType(fileds)

    val df = spark.createDataFrame(rdd1, schema)
    df.printSchema()
    df.createOrReplaceTempView("t")
    sql("select * from t where area > 500 and area < 1800").collect().foreach(row=>println(row.getString(0)+"-"+row.getString(1)))

  }

  def readParquet: Unit ={
    import spark.sqlContext._
    import spark.sqlContext.implicits._
//    val df = spark.read.parquet("hdfs://master:8082/datas/parqeut")
    val df = spark.read.parquet("hdfs://master:8082/datas/parquet")
    df.printSchema()
    df.createOrReplaceTempView("t")
    sql("select * from t where area < 555").collect().foreach(row=>println(row.getInt(0)+"_"+row.getInt(1)))
  }

  def readMysql: Unit ={
    import spark.sqlContext._
    import spark.sqlContext.implicits._
    val df = spark.read.jdbc("jdbc:mysql://47.52.117.69:3306/spark211_text","df",getMysqlProperty)
    df.printSchema()
    df.createOrReplaceTempView("t")
    sql("select * from t where area > 555").collect().foreach(row=>println(row.getInt(0)+"_"+row.getInt(1)))

  }


  def main(args: Array[String]): Unit = {
//    saveCsv()
//    saveJson()
//    saveParquet
//    saveHDFS
//    saveMYSQL
//    readCSV
//    readJSON
//    readRDD
//    readText
//    readParquet
//    readMysql

    spark.stop()
  }

}

case class Area(side:Int,area:Int)
