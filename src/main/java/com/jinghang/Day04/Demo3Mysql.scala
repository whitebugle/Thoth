package com.jinghang.Day04

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Demo3Mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)


    val driver="com.mysql.cj.jdbc.Driver"
    val url="jdbc:mysql://localhost:3306/mybatis01?serverTimezone=UTC"
    val username="root"
    val password="wu555555"
/*    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _*/

    val userRdd: JdbcRDD[(Int, String, String)] = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      },
      "select * from user where id >=? and id<=?",
      40,
      50,
      1,
      r => (r.getInt(1), r.getString(2), r.getString(4))
    )
    userRdd.foreach(println(_))

    val nextRdd:RDD[(Int,String,String)]= sc.parallelize(Array((65,"shushu","男"),(70,"shenshen","女")))

    nextRdd.foreachPartition(it=>{
      Class.forName(driver)
      val connection = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/mybatis01?serverTimezone=UTC",
        "root",
        "wu555555"
      )
      it.foreach(data=>{
        val insert = connection.prepareStatement("insert into user (id,username,sex) values (?,?,?)")
        insert.setInt(1,data._1)
        insert.setString(2,data._2)
        insert.setString(3,data._3)
        insert.executeUpdate()
      })
    })
    sc.stop()
  }

}
