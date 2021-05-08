package com.jinghang.Day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2ip {

  def search(ipl: Long, value: Array[(Long, Long, String)]):String={
   var start:Long=0L
    var end:Long=value.size-1
    while (start<=end){
      val mid:Int=((start+end)/2).toInt
      if(ipl>=value(mid)._1 && ipl<=value(mid)._2){
       return value(mid)._3
    }else if (ipl < value(mid)._1){
      end=mid-1
    }else if(ipl > value(mid)._2){
      start=mid+1
    }
  }
    "未知地点"
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ipRdd: Array[(Long, Long, String)] = sc.textFile("E:\\jinghang\\课件03\\day13\\ip.txt").map(i => {
      val ips: Array[String] = i.split("\\|")
      (ips(2).toLong, ips(3).toLong, ips(6))
    }).collect()
    val ipbc: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRdd)

    val top3: Array[(String, Int)] = sc.textFile("E:\\jinghang\\课件03\\day13\\20090121000132.394251.txt").map(i => {
      val ip: String = i.split("\\|")(1)
      val iparr: Array[String] = ip.split("\\.")
      val ipl = iparr(0).toLong * 256 * 256 * 256 + iparr(1).toLong * 256 * 256 + iparr(2).toLong * 256 + iparr(3).toLong
      val add: String = search(ipl, ipbc.value)
        (add, 1)
    }).reduceByKey(_ + _).sortBy(_._2, false, 1).take(3)
   top3.foreach(println(_))
    sc.stop()
  }

}
