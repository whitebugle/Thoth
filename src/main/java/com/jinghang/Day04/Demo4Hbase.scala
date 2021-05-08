package com.jinghang.Day04

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo4Hbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

     val sc = new SparkContext(conf)

    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum","hadoop02,hadoop03,hadoop04")
    hconf.set(TableInputFormat.INPUT_TABLE,"spark")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val count: Long = hbaseRDD.count()

    hbaseRDD.foreach{
      case (_,result)=>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val age: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
        println("RowKey:" + key + ",Name:" + name + ",age:" + age)
    }
  }

  def test: Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val sc = new SparkContext(conf)
    val hconf = HBaseConfiguration.create()
    hconf.set("habase.zookeeper.quorum","hadoop02,hadoop03,hadoop04")
    hconf.set(TableInputFormat.INPUT_TABLE,"ss")
    val unit: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    unit.foreach(a=>{
      val key: String = Bytes.toString(a._2.getRow)
      val name: String = Bytes.toString(a._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      val age: String = Bytes.toString(a._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
      println("RowKey:" + key + ",Name:" + name + ",age:" + age)
    }

    )
  }
}
