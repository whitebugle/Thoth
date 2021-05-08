package com.jinghang.Day05

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Demo1Persist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
   /* conf.set("spark.kryo.registrator","myregistrator")*/
    conf.registerKryoClasses(Array(classOf[Mytestper]))
    val sc = new SparkContext(conf)
    val rdd01: RDD[Mytestper] = sc.parallelize(Array(new Mytestper("1",1),new Mytestper("2",2),new Mytestper("3",3)))
    println(rdd01.count())
    rdd01.persist(StorageLevel.MEMORY_ONLY_SER)
    println(rdd01.count())
  }
}
class Mytestper (na:String,ag:Int)extends Serializable{
  val name:String=na
  val age:Int=ag
}
