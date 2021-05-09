package com.jinghang.Day12

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkatoHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val sc = ss.sparkContext

    val streamc: StreamingContext = new StreamingContext(sc,Seconds(5))

    streamc.checkpoint("hdfs://hadoop02:9000/test/0509")

    val Kafkaparams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop02:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    ss.sql("create table if not exists default.test (name String,num int,time bigint) row format delimited fields terminated by '\t'")


    val topic=Array("first")
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, Kafkaparams/*Map(("a", "b"))*/))
    kafkaDS.filter(!_.value().isEmpty).flatMap(a=>{
      a.value().split(" ")
    }).map((_,1)).updateStateByKey((name:Seq[Int],have:Option[Int])=>{
      Option(name.sum+have.getOrElse(0))
    }).foreachRDD(a=>{
      import ss.implicits._
      val rdd01 = a.map(a=>{(a._1,a._2,System.currentTimeMillis())})
      val df: DataFrame = rdd01.toDF("name","num","time")
      df.createOrReplaceTempView("p")
      ss.sql(
        """
          |insert into
          |table
          |default.test
          |select * from p
        """.stripMargin)
    })
    streamc.start()
    streamc.awaitTermination()
  }

}
