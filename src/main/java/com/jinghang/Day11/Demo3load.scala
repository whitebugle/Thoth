package com.jinghang.Day11

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo3load {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = ss.read.format("json").load("E:\\jinghang\\课件03\\day18\\json_data\\movie.json")
    df.show()

    /*  ss.read.json()*/

    //load默认加载parquet格式
    /*ss.read.load("")*/
    ss.stop()
  }

}
