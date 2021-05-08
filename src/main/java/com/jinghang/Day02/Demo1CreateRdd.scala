package com.jinghang.Day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1CreateRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val array = Array(1,2,3,4,5)
    val rdd0: RDD[Int] = sc.parallelize(array)
    println(rdd0.getNumPartitions)

    val rdd1: RDD[Int] = sc.makeRDD(array)

    val rdd2: RDD[Int] = sc.parallelize(array,5)

    println(rdd2.getNumPartitions)

    val rdd4: RDD[(Int, Int)] = rdd2.map((_,1))

    val rdd5: RDD[(Int, Int)] = sc.parallelize(Array((1,2),(2,3),(4,5)))
    sc.stop()
  }
}
