package com.jinghang.Day09

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object Demo1SparkSession {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
   /* val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
                                       //2.0之前如此
    val sQLContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)*/

    //2.0之后直接创建sparksession,内部包含sqlcontext与hivecontext对象
    val session: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    /*  .config("spark.sql.warehouse.dir", warehouseLocation)  //此配置为使用内置 Hive 需要指定一个 Hive 仓库地址。若使用的是外部 Hive，则需要将 hive-site.xml 添加到 ClassPath 下。
      .enableHiveSupport()*/

    val df: DataFrame = session.read.json("E:\\jinghang\\课件03\\day16\\people.json")
    df.printSchema()
    df.select((df.col("age")+100)as("agg")).show()
    df.select("name","age").groupBy("name").count().show()
    println("-----------------------------------")
//sql风格
    df.createOrReplaceTempView("p")
     session.sql("select * from p ").show()
    session.sql("select name,age from(select name,age,row_number()over(order by age desc)as top from p )t1 where t1.top<=3").show()
   println("--------------------------------")
    session.sql(
      """
        |select
        |name,
        |max(age) ma
        |from p
        |group by name
        |order by ma desc
        |limit 3
      """.stripMargin).show()
    session.stop()

  }
}
