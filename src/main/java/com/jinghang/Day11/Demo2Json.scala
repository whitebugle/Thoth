package com.jinghang.Day11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Demo2Json {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext
/*    val df: DataFrame = ss.read.json("")*/
    val rdd01: RDD[String] = sc.textFile("E:\\jinghang\\课件03\\day18\\json_data\\test.json")
    rdd01.map(line=>{
      val mapper = new ObjectMapper()
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      val node: JsonNode = mapper.readTree(line)
      val movieid = node.get("movie").asInt()
      val rate = node.get("rate").asInt()
      var name=""
     if (node.has("name")) {
       name =node.get("name").asText()
     }
      Topmovie(movieid,rate,name)
    }
    ).foreach(println(_))
  }
}
case class Topmovie(movie:Integer,rate:Integer,name:String){
//  override def toString: String = s"$movie ,$rate"
}
