package com.jinghang.Day07

import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo1SSWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    val stream01: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop02",9999)

    stream01.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
