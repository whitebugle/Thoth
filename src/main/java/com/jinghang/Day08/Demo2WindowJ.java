package com.jinghang.Day08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Demo2WindowJ {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Demo1UpdataStateByKeyJ").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        jssc.checkpoint("E:\\jinghang\\课件03\\day15\\0428");
        JavaReceiverInputDStream<String> ds01 = jssc.socketTextStream("hadoop02", 9999);
        ds01.flatMap(a-> Arrays.asList(a.split(" ")).iterator())
                .mapToPair(a->new Tuple2<>(a,1))
                .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                },Durations.seconds(10),Durations.seconds(5))
                .print();
        jssc.start();
        jssc.awaitTermination();
    }
}
