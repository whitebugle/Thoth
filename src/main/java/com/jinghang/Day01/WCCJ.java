package com.jinghang.Day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WCCJ {
    public static void main(String[] args) {

        simpleread(args);


    }

    public static void simpleread(String[]args){
        SparkConf conf = new SparkConf().setAppName("WCCJ");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = jsc.textFile(args[0]);
        JavaRDD<String> stringJavaRDD = javaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = stringJavaRDD.mapToPair(line -> new Tuple2<>(line, 1));
        JavaPairRDD<String, Integer> byKey = pairRDD.reduceByKey((a, b) -> a + b);
        byKey.foreach(x-> System.out.println(x));
        byKey.saveAsTextFile(args[1]);
        jsc.stop();
    }

    public static void allread(String[] args){
        SparkConf conf = new SparkConf().setAppName("WCCJ");
        JavaSparkContext JSC = new JavaSparkContext(conf);
        JavaRDD<String> rdd = JSC.textFile(args[0]);

        JavaRDD<String> arrayrdd = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairRDD = arrayrdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> pairRDDres = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        pairRDDres.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1()+":"+stringIntegerTuple2._2());
            }
        });
        pairRDDres.saveAsTextFile(args[1]);

        JSC.stop();
    }
}
