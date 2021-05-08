package com.jinghang.Day03

import org.apache.spark.{SparkConf, SparkContext}

object Demo2Action {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo2Action").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(1,2,5,3,6,7,8,4))
    val i: Int = rdd1.reduce(_+_)
    val ints: Array[Int] = rdd1.collect()
    println(ints)
    val i1 = rdd1.first()
    val take: Array[Int] = rdd1.take(3)
    rdd1.takeOrdered(3).foreach(println(_))

    rdd1.fold(0)(_+_)

    rdd1.aggregate(0)(_+_,_+_)

    val rddm = rdd1.map((_,1))
    val intToLong: collection.Map[Int, Long] = rddm.countByKey()


  /*  rdd1.saveAsTextFile("")
    rddm.saveAsSequenceFile("")
    rddm.saveAsNewAPIHadoopFile("")*/
    rdd1.foreach(println(_))

    rdd1.foreachPartition(println(_))

    sc.stop()
  }
}
