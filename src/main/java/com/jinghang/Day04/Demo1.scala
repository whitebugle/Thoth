package com.jinghang.Day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

      val sc = new SparkContext(conf)

      val search = new Search("a")

      val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "jinghang"))
    val rdd2: RDD[String] = search.getMatch1(rdd)
      search.getMatch2(rdd).foreach(println(_))

    println(rdd.toDebugString)
    println(rdd.dependencies)
    println(rdd2.dependencies)
    sc.stop()
  }


  //rdd调用或传入个人类需要实现Serializable接口
  class Search (s:String)extends Serializable {
    def getMatch1(rdd: RDD[String]): RDD[String] ={
      rdd.filter(_.contains("a"))
    }

    def getMatch2(rdd:RDD[String]): RDD[String] ={
      rdd.filter(_.contains(s))
    }
  }
}
