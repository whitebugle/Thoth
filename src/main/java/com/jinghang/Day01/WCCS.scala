package com.jinghang.Day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WCCS {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //创建sparkConf对象
    //local[2] 本地模式 是哟个2个cpu的核数来模拟计算
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    //创建sparkContext对象
    val sc = new SparkContext(conf)

    val rdd01: RDD[String] = sc.textFile(args(0))

    val resRdd = rdd01.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    resRdd.saveAsTextFile(args(1))

    sc.stop()
  }

}
