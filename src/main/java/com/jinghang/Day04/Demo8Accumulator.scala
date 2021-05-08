package com.jinghang.Day04

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Demo8Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd01 = sc.parallelize(Array(5,5,5,5,5,5,5,8,9))
    val ava = new AvgAccumulator
    sc.register(ava)
    rdd01.map(i=>{
      ava.add(i)
      i
    }).collect()
    println(ava.value)
    sc.stop()
  }

}

class AvgAccumulator extends AccumulatorV2[Int,(Long,Double)]{

  private var list=ListBuffer[Double]()

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[Int, (Long,Double)] = {
    val accumulator = new AvgAccumulator
    accumulator.list++list
    accumulator
  }

  override def reset(): Unit = list.clear()

  override def add(v: Int): Unit = {
    list += v
  }

  override def merge(other: AccumulatorV2[Int, (Long,Double)]): Unit = {
    if (other.value._1!=0){
    list+= other.value._1*other.value._2
    for(i <- 0L until   other.value._1-1){
       list+=0L
     }
  }
  }

  override def value: (Long,Double) = {
    (list.size,list.sum/list.size)
  }


}
