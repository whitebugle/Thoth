package com.jinghang.Day07

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Demo2Source {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    //ssc.queueStream(new mutable.Queue[RDD[Nothing]]())
    //ssc.textFileStream("")
    //ssc.binaryRecordsStream()
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    ssc.queueStream(queue).print()
    ssc.start()

    for (i<- 1 to 5){
      queue += sc.makeRDD(1 to 3)
      Thread.sleep(3000)
    }
    ssc.awaitTermination()
  }

}
