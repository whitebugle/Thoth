package com.jinghang.Day08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Demo1UpdataStateByKeyJ {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Demo1UpdataStateByKeyJ").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        jssc.checkpoint("E:\\jinghang\\课件03\\day15\\0428");
        JavaReceiverInputDStream<String> ds01 = jssc.socketTextStream("hadoop02", 9999);
        ds01.flatMap(a-> Arrays.asList(a.split(" ")).iterator())
                .mapToPair(a->new Tuple2<>(a,1))
                .updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                        Integer sum =0;
                        for (Integer integer : v1) {
                            sum+=integer;
                        }
                        sum += v2.orElse(0);
                        return v2.of(sum);
                    }
                }).print();
        jssc.start();
        jssc.awaitTermination();
    }
}
