package com.jinghang.Day08

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo3Transform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))


    ssc.checkpoint("E:\\jinghang\\课件03\\day15\\0428")
    val list: RDD[(String, Boolean)] = sc.parallelize(Array(("huang"->true),("qian"->true),("sun"->true)))

    val ds01: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop02",9999)
    ds01.transform(rdd01=> {
      val joinrdd: RDD[(String, (Boolean, Option[Boolean]))] = rdd01.flatMap(_.split(" ")).map((_,false)).leftOuterJoin(list)
      joinrdd.filter(_._2._2.getOrElse(false)).map(a=>(a._1,1))
      }).updateStateByKey((a:Seq[Int],b:Option[Int])=>{
      var sum = a.sum
      sum+= b.getOrElse(0)
      Option(sum)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
