package com.jinghang.Day11

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Udf {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()


    val df = ss.read.json("E:\\jinghang\\课件03\\people.json")
    df.createOrReplaceTempView("p")
    ss.udf.register("avgfun",new Avgage)
    ss.sql("select name,avgfun(age) from p group by name").show()
    ss.stop()
  }


}
class Avgage extends UserDefinedAggregateFunction{
  //输入的结果
  override def inputSchema: StructType = {
    StructType(Array(StructField("age",IntegerType,true)))
  }

  //中间缓存的结果
  override def bufferSchema: StructType = {
    StructType(Array(StructField("sum",IntegerType,true),StructField("num",IntegerType,true)))
  }

  //输出结果的类型
  override def dataType: DataType = DoubleType

  // 对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = false

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0
    buffer(1)=0
  }

  //单个数据更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + input.getAs[Int](0)
    buffer(1) = buffer.getAs[Int](1) + 1
  }

  //不同分区的结果合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    buffer1(1) = buffer1.getAs[Int](1) + buffer2.getAs[Int](1)
  }

  //输出结果
  override def evaluate(buffer: Row): Any = {buffer.getAs[Int](0).toDouble/buffer.getAs[Int](1)}
}
