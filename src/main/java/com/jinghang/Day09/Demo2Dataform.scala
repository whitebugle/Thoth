package com.jinghang.Day09

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Demo2Dataform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val sps: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .getOrCreate()
    val lineRdd: RDD[String] = sc.textFile("E:\\jinghang\\课件03\\day16\\student01.txt")

    val stuRdd = lineRdd.map(line => {
      val linarr = line.split(" ")
      Student(linarr(0), linarr(1), linarr(2).toInt)
    })
    //RDD通过包装样例类对象及导入session对象的隐式转换实现直接转换dataframe
    import sps.implicits._
    val dframe: DataFrame = stuRdd.toDF

    val dset: Dataset[Student] = stuRdd.toDS

    val sturdd1: RDD[Student] = dset.rdd

    val sturdd2: RDD[Row] = dframe.rdd

    val dset1: Dataset[Student] = dframe.as[Student]

    dframe.createOrReplaceTempView("stu")

    sps.sql(
      """
        |select
        |classess,name,score
        |from(
        |select
        |*,row_number()over(partition by classess order by score desc) top
        |from stu )t1
        |where t1.top<4
      """.stripMargin).show()
  }

}
case class Student(classess:String,name:String,score:Int)