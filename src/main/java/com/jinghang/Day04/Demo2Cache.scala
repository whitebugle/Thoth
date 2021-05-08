package com.jinghang.Day04

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Demo2Cache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd01 = sc.textFile("E:\\jinghang\\课件03\\day10\\agent.log")

    //
    //=persist(StorageLevel.MEMORY_ONLY)
    rdd01.cache()

    /*
  如何选择？
  1.优先保存内存
  2.内存序列化
  3.内存和磁盘
   */
    rdd01.persist(StorageLevel.MEMORY_ONLY)


    sc.setCheckpointDir("E:\\jinghang\\课件03\\day10\\agent.log")
    rdd01.checkpoint()
    sc.stop()
  }

}
