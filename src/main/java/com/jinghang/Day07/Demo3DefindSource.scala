package com.jinghang.Day07

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.net.Socket
import java.nio.charset.StandardCharsets
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object Demo3DefindSource {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //创建StreamingContext对象
    val ssc = new StreamingContext(sc, Seconds(5))

    val Dstream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop02",9999))


    Dstream.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2){
  override def onStart(): Unit = {
    new Thread("Socket Receiver"){
      override def run(): Unit = {
      receive()
      }
    }.start()
  }

  def  receive(): Unit ={
    val socket = new Socket(host,port)
    var str:String=null
    val reader: BufferedReader = new BufferedReader(new InputStreamReader( socket.getInputStream,StandardCharsets.UTF_8))

    str = reader.readLine()

    while (!isStopped()&&str!=null) {
        store(str)
      reader.readLine()
    }
    reader.close()
    socket.close()
    restart("restart")
  }

  override def onStop(): Unit = {

  }
}