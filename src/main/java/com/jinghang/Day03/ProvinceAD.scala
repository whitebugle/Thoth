package com.jinghang.Day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceAD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd01 = sc.textFile("E:\\jinghang\\课件03\\day10\\agent.log")
    val rdd02 = sc.textFile("E:\\jinghang\\课件03\\day10\\agent1.log")

    val pAt: RDD[((String, String), Int)] = rdd01.map(a => {
      val strings: Array[String] = a.split(" ")
      val province = strings(1)
      val Ad = strings(4)
      ((province, Ad), 1)
    })
    val pAs: RDD[((String, String), Int)] = pAt.reduceByKey(_+_)
    val pAs1: RDD[(String, (String, Int))] = pAs.map(a => {
      (a._1._1, (a._1._2, a._2))
    })
    val PAS: RDD[(String, Iterable[(String, Int)])] = pAs1.groupByKey()
    val PAS3: RDD[(String, List[(String, Int)])] = PAS.mapValues(a => {
      a.toList.sortWith(_._2 > _._2).take(3)
    })
    PAS3.foreach(println(_))

    val pid = rdd02.map(a => {
      val strings = a.split(" ")
      val id = strings(0)
      val name = strings(1)

      (id, name)
    })

    PAS3.leftOuterJoin(pid).map(a=>{
      (a._2._2.getOrElse(null),a._2._1)
    }).foreach(println(_))

  sc.stop()
  }

}
