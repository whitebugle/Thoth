package com.jinghang.Day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Demo2Transformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    mapPartition(sc)

    sc.stop();
  }

  def map(sc:SparkContext)={
    val numRdd=sc.parallelize(1 to 10)

    numRdd.map(_*10).foreach(println(_))
  }

  def mapPartition(sc:SparkContext)={
    val numrdd = sc.parallelize(1 to 10)
    numrdd.mapPartitions(a=> {
      val list = ListBuffer[Int]()
    val it = a.toIterator
      while (it.hasNext){
        list+=it.next()*10
      }
      list.toIterator
    }
    )
      .foreach(println(_))
  }

  def flaMap(sc:SparkContext)={
     val array = Array("are u ok")
    val lineadd = sc.parallelize(array)
    val value: RDD[String] = lineadd.flatMap(_.split(" "))
    val value1: RDD[Array[String]] = lineadd.map(_.split(" "))
  }

  def glom(sc:SparkContext)={
    val numRdd = sc.parallelize(1 to 10)
    //glom 将每个分区的元素 变成一个集合
    val value: RDD[Array[Int]] = numRdd.glom()

    value.foreach(arr=>arr.toBuffer)
  }

  def groupby(sc:SparkContext)={
    val arr = Array("are u ok","how are u","how old are you","how you you u how","fine thank you")
    val lineRdd = sc.parallelize(arr)

    val rdd01 = lineRdd.flatMap(_.split(" "))
    val pairRdd = rdd01.map((_, 1))

    pairRdd.reduceByKey(_+_)


    val value: RDD[(String, Iterable[(String, Int)])] = pairRdd.groupBy(_._1)


    //Iterable[Int] 相同 key 的value 的集合
    val value2: RDD[(String, Iterable[Int])] = pairRdd.groupByKey()

    val rdd02 = value.map(a => {
      val key = a._1
      val va: Iterable[(String, Int)] = a._2
      var sum = 0
      va.foreach(sum += _._2)
      (key, sum)
    }
    )
    rdd02.sortByKey(false,1) //false 降序  1 指定一个分区
    rdd02.sortBy(_._2,false,1).take(5)foreach(println(_))

    value2.map(a=>{
      val key=a._1
      val VI: Iterable[Int] = a._2
      (key,VI.sum)
    }
    )

  }

  def filter (sc:SparkContext): Unit ={

    val numRdd = sc.parallelize(1 to 10)
    //留下返回为true 的数据
    numRdd.filter(_%2==0).foreach(println(_))
  }

  def sample (sc:SparkContext): Unit ={
    val numRdd = sc.parallelize(1 to 10)
    //withReplacement: Boolean,   是否返回
     //fraction: Double,  抽样的比例  0-1
    val unit = numRdd.sample(true,0.5)
  unit.foreach(println(_))


    //collect 是action 算子 将workwer 计算的结构以数组的形式发送到driver
    val ints: Array[Int] = unit.collect()
    println(ints.toBuffer)
  }

  def mappartitionwithIndex(sc:SparkContext): Unit ={
    val numRdd=sc.parallelize(1 to 10)
    numRdd.mapPartitionsWithIndex((a,b)=>{
      val buffer = new ListBuffer[String]()
      while(b.hasNext){
        buffer += a+":"+b.next()
      }
      buffer.toIterator},false).foreach(println(_))
  }
}
