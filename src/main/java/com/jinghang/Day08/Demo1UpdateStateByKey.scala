package com.jinghang.Day08

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo1UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))


    val Ds01: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop02",9999)
    Ds01.flatMap(_.split(" ")).map((_,1)).updateStateByKey((value:Seq[Int],op:Option[Int])=>{
     //value :当前batch的数据  option:之前batch累计的结果
      var sum:Int=value.sum
      sum += op.getOrElse(0)
      Option(sum)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
