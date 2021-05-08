package com.jinghang.Day07


import kafka.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo4Kafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val topics = Array("first")

    val Kafkaparams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop02:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )


    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, Kafkaparams)
    )
    stream.map(record=>
      //      record.checksum()
      //      record.key()
      //      record.topic()
      //      record.partition()
      //      record.offset()  //偏移量
      //      record.timestamp()
      record.value()
    )
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()

  }

}
