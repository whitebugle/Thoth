package com.jinghang.Day10

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
;

object SparkonHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val session: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = session.sparkContext//注意,在集群上运行hive时，sc对象不能直接new,否则找不到hive元数据
    val df = session.sql(
      """select
        |*
        |from
        |student
        |limit 5
      """.stripMargin)
   df.show()
   session.stop()
    sc.stop()
  }

}
