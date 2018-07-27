// https://blog.csdn.net/lyzx_in_csdn/article/details/79149502

package com.lyzx.day31

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}

class T1 {

  /**
    * 关于SparkStreaming的一个demo
    * @param ssc
    */
  def f1(ssc:StreamingContext): Unit ={
      //返回值类型是ReceiverInputDStream
      //192.168.29.160是虚拟机(linux)的IP地址
      //使用nc -lk 9999命令输入数据,当输入nc -lk 9999时就表示在192.168.29.160这台机器上的9999端口启动了一个socket server

      //监听192.168.29.160的9999端口
      val line = ssc.socketTextStream("192.168.29.160",9999)

      //对接受到的数据做worldCount并打印
      line.flatMap(_.split(" "))
          .map((_,1))
          .reduceByKey(_+_)
          .print()

    //开启流式处理
    ssc.start()
    //等待
    ssc.awaitTermination()
  }

  /**
    * Note that, if you want to receive multiple streams of data in parallel in your streaming application,
    * you can create multiple input DStreams (discussed further in the Performance Tuning section).
    * This will create multiple receivers which will simultaneously receive multiple data streams.
    * But note that a Spark worker/executor is a long-running task,
    * hence it occupies one of the cores allocated to the Spark Streaming application. Therefore,
    * it is important to remember that a Spark Streaming application needs to be allocated enough cores
    * (or threads, if running locally) to process the received data,
    * as well as to run the receiver(s)
    *
    * 如果你想在应用程序中并行的接受多个流那么你就需要创建多个DStream一起接收流式数据
    * 但是要注意这是一个长期的任务，所以该应用程序需要被分配一个cpu的核
    * 因此记住应用程序要被分配足够的cpu资源(或者线程数)这一点很重要
    *
    * @param ssc
    */
  def f2(ssc:StreamingContext): Unit ={
      val line160 = ssc.socketTextStream("192.168.29.160",9999)
      val line161 = ssc.socketTextStream("192.168.29.161",9999)

    line160.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    line161.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    //开启流式处理
    ssc.start()
    //等待
    ssc.awaitTermination()
  }

//  def f3(ssc:StreamingContext): Unit ={
//    val line = ssc.fileStream("D:\\1")
//    line.flatMap(x=>x._1).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
}

object T1{
  def main(args: Array[String]): Unit = {
      //这儿的setMaster方法里要写local[N] N>=2,原因是至少启动2个线程，一个线程是receiver Task即从外部的
      //数据源接受数据,剩下的至少一个线程用来处理receiver Task接受的数据
      val conf = new SparkConf().setAppName("day31").setMaster("local[4]")

      //创建Streaming上下文
      val ssc = new StreamingContext(conf,Seconds(5))
      val t = new T1
//      t.f1(ssc)
//        t.f2(ssc)
        t.f3(ssc)
      ssc.stop()

    //ssc.stop(false)仅仅只关闭ssc
  }
}
