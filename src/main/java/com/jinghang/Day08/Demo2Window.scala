package com.jinghang.Day08

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo2Window {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc,Seconds(5))

    ssc.checkpoint("E:\\jinghang\\课件03\\day15")
    val ds01: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop02",9999)
    ds01.flatMap(_.split(" ")).map((_,1))
      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(5)).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
