package com.jinghang.Day04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo7Broadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val value: RDD[(String, Int)] = sc.parallelize(Array("ha asd","sad asd","sad 1")).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    val tuples: Array[(String, Int)] = value.collect()
    val bc: Broadcast[Array[(String, Int)]] = sc.broadcast(tuples)
    val rdd02 = sc.parallelize(Array("are","you","sad")).map((_,true))


    rdd02.map(a=>{
      val bcv: Array[(String, Int)] = bc.value
      val it = bcv.toIterator
      var t3=0
      while (it.hasNext) {
        val tuple = it.next()
        if (a._1==tuple._1) {
          t3= tuple._2
        }
      }
      (a._1,a._2,t3)
    }).filter(_._3!=0).foreach(println(_))

    sc.stop()
  }

}
