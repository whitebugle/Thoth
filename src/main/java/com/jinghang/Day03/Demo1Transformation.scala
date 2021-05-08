package com.jinghang.Day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner,HashPartitioner, SparkConf, SparkContext}

object Demo1Transformation {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //distinct(sc)

  //repartition(sc)
    //aggregatebykey(sc)
    //join(sc)
    cogroup(sc)
    sc.stop()
  }

  def distinct(sc:SparkContext): Unit ={
    val arr = Array("are u ok","how are u","how old are you","how you you u how","fine thank you")
    val lineRdd = sc.parallelize(arr)
    lineRdd.flatMap(_.split(" ")).distinct().foreach(println(_))
  }

  def repartition(sc:SparkContext): Unit ={
    val arr = Array("are u ok","how are u","how old are you","how you you u how","fine thank you")
    val lineRdd = sc.parallelize(arr,3)

    lineRdd.repartition(5)
    lineRdd.repartition(2)

    val value = lineRdd.coalesce(5).getNumPartitions
    println(value)
    //coalesce 默认 增加分区时不进行 shuffle,分区数不会增加，需要手动开启
    lineRdd.coalesce(5,true)
    lineRdd.coalesce(2)

  }
  def union (sc:SparkContext): Unit ={
    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(3 to 7)
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    //union 求并集，会重复
    rdd3.foreach(println(_))

    //subtract 差集
    rdd1.subtract(rdd2)
      .foreach(println(_))//1,2

    //intersection 交集
    rdd1.intersection(rdd2)
      .foreach(println(_))//3,4,5

    //笛卡尔积
    rdd1.cartesian(rdd2)
      .foreach(println(_))

    //zip 拉链  spark 的拉链要求 两个rdd元素数量相同
    rdd1.zip(rdd2)
      .foreach(println(_))
  }

  def partitionby(sc:SparkContext): Unit ={
    val arr = Array("are u ok","how are u","how old are you","how you you u how","fine thank you")
    val lineRdd = sc.parallelize(arr,3)
    val pairRdd = lineRdd.flatMap(_.split(" ")).map((_,1))


    //partitionby针对key-value 类型,对key进行分组
    pairRdd.partitionBy(new HashPartitioner(3))

    pairRdd.partitionBy(new Mypartition(3))
  }

  class Mypartition(numpartition:Int)extends Partitioner{
    override def numPartitions: Int = numpartition

    override def getPartition(key: Any): Int = {
      if (key.isInstanceOf[Int]) {
        (key.asInstanceOf[Int]+1)%numPartitions
      }else{
        key.hashCode()%numPartitions
      }
    }
  }

  def aggregatebykey(sc:SparkContext)={
    val arr = Array("are u ok","how are u","how old are you","how you you u how","fine thank you")
    val lineRdd = sc.parallelize(arr,3)
    //分区分好后，除非后续指定及含有shuffle的操作，否则不变
    val pairrdd: RDD[(String, Int)] = lineRdd.flatMap(_.split(" ")).map((_,1))

    pairrdd.reduceByKey(_+_).foreach(println(_))

    println("-------------------------")
    //初始值是每个分区都加上初始值
    pairrdd.foldByKey(10)(_+_).foreach(println(_))
    println("-------------------------")

    //初始值是每个分区都加上初始值
    //第一个_+_是每个分区的聚合  第二个_+_ 是分区之间的聚合
    pairrdd.aggregateByKey(100)(_+_,_+_)

    //初始值是每个分区都加上初始值
    pairrdd.combineByKey(i=>i+1,(a:Int,b:Int)=>a+b,(a:Int,b:Int)=>a+b)
  }

  def join(sc:SparkContext)={
    val rdd01 = sc.parallelize(Array("are","u","ok")).map((_,1))
    val rdd02 = sc.parallelize(Array("are","you","ok")).map((_,true))
    val rddjoin: RDD[(String, (Int, Boolean))] = rdd01.join(rdd02)
    rddjoin.foreach(println(_))

    rdd01.leftOuterJoin(rdd02).foreach(println(_))
    rdd01.rightOuterJoin(rdd02).foreach(println(_))
    val rddfull: RDD[(String, (Option[Int], Option[Boolean]))] = rdd01.fullOuterJoin(rdd02)

    rddfull.map(a=>{
      val key = a._1
      val v1 = a._2._1.getOrElse(0)
      val v2 = a._2._2.getOrElse(true)
      (key,v1,v2)
    }).foreach(println(_))
  }

  //按key全聚合
  def cogroup(sc:SparkContext)={
    val rdd01 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd02 = sc.parallelize(Array((1,4),(2,5),(3,6)))

    val rddco: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd01.cogroup(rdd02)
    rddco.foreach(println(_))
  }

}
