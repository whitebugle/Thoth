package com.jinghang.Day04

import org.apache.spark.{SparkConf, SparkContext}

object Demo6Add {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val acc = sc.longAccumulator("acc")
    val Addrdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    Addrdd.map(i=>{
      acc.add(i)
      i
    }).collect()
    //注意需collect将数据返回driver ，累加器才能取到数据
    println(acc.value)
    sc.stop()
  }

}
