package com.jinghang.Day07;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Demo1SSWCJ {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Demo1SSWCJ").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        JavaReceiverInputDStream<String> stream = jssc.socketTextStream("hadoop02", 9999);

        stream.flatMap(a-> Arrays.asList(a.split(" ")).iterator())
                .mapToPair(a->new Tuple2<>(a,1))
                .reduceByKey((a,b)->a+b).print();
        jssc.start();
        jssc.awaitTermination();
    }
}
