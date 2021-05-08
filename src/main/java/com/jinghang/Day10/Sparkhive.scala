package com.jinghang.Day10

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object Sparkhive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val df = hc.sql(
      """select
        |*
        |from
        |student
        |limit 5
      """.stripMargin)
    val rows: Array[Row] = df.collect()
    println(rows.toBuffer)
    sc.stop()
  }

}
