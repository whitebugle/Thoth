package com.jinghang.Day08;

import com.sun.media.jfxmedia.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Demo3TransoformJ {
    public static void main(String[] args) throws InterruptedException {
        Logger.setLevel(Logger.ERROR);
        SparkConf conf = new SparkConf().setAppName("Demo1UpdataStateByKeyJ").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        JavaPairRDD<String, Boolean> listrdd = jsc.parallelize(Arrays.asList("zhao", "qian", "huang", "li")).mapToPair(a -> new Tuple2<>(a, true));
        jssc.checkpoint("E:\\jinghang\\课件03\\day15\\0428");
        JavaReceiverInputDStream<String> ds01 = jssc.socketTextStream("hadoop02", 9999);
        ds01.flatMap(a-> Arrays.asList(a.split(" ")).iterator())
                .mapToPair(a->new Tuple2<>(a,false))
                .transform(new Function<JavaPairRDD<String, Boolean>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, Boolean> v1) throws Exception {
                        JavaPairRDD<String, Tuple2<Boolean, Optional<Boolean>>> joinrdd = v1.leftOuterJoin(listrdd);
                         return  joinrdd.mapToPair(new PairFunction<Tuple2<String, Tuple2<Boolean, Optional<Boolean>>>, String, Boolean>() {
                            @Override
                            public Tuple2<String, Boolean> call(Tuple2<String, Tuple2<Boolean, Optional<Boolean>>> stringTuple2Tuple2) throws Exception {
                                String key = stringTuple2Tuple2._1;
                                Boolean value = stringTuple2Tuple2._2()._2().orElse(false);
                                return new Tuple2<>(key,value);
                            }
                        }).filter(a->a._2).map(a->a._1);

                    }
                }).mapToPair(a->new Tuple2<>(a,1)).reduceByKey((a,b)->a+b).print();
        jssc.start();
        jssc.awaitTermination();
    }
}
