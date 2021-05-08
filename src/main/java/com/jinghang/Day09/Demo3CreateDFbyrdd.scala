package com.jinghang.Day09


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Demo3CreateDFbyrdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val sps: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sturdd:RDD[String] = sc.textFile("E:\\jinghang\\课件03\\day16\\student01.txt")
    val rowRdd: RDD[Row] = sturdd.map(line => {
      val linarr = line.split(" ")
      Row(linarr(0), linarr(1), linarr(2).toInt)
    })
    val schema: StructType = StructType {
      Array(
        StructField("classess", StringType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }

    //通过RowRDD 转换
    val df: DataFrame = sps.createDataFrame(rowRdd,schema)

    df.show()

  }

}
