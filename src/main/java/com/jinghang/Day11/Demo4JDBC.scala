package com.jinghang.Day11

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Demo4JDBC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()


    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","wu555555")
    val frame: DataFrame = ss.read.jdbc("jdbc:mysql://localhost:3306/db1?serverTimezone=UTC","student",properties)
    frame.show()


    println("-----------------------------------------")
    val df = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/db1?serverTimezone=UTC")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "wu555555")
      .load()
    df.show()



    import ss.implicits._
    val value: Dataset[student] = df.as[student]
    val dsin: Dataset[student] = value.map(a => {
      student(a.age, a.name, a.sex, a.id + 22, a.Math, a.english)
    })
    val framein = dsin.toDF()
   // df.write.jdbc("jdbc:mysql://localhost:3306/db1?serverTimezone=UTC","student",properties)
/*    df.write.json("E://jinghang//b.json")*/
    framein.write.format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/db1?serverTimezone=UTC")
        .option("dbtable","student")
        .option("user","root")
        .option("password","wu555555")
        .mode(SaveMode.Append)
        .save()
    ss.stop()
  }
}

case class student(age:Integer,name:String,sex:String,id:Integer,Math:Integer,english:Integer)
