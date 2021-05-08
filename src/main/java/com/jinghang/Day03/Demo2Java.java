package com.jinghang.Day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Demo2Java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo2Java").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> jrdd = jsc.parallelize(Arrays.asList(1, 2, 5, 3, 6, 7, 8, 4));

        Integer reduce = jrdd.reduce((a, b) -> a + b);
        List<Integer> collect = jrdd.collect();
        long count = jrdd.count();
        Integer first = jrdd.first();
        List<Integer> take = jrdd.take(3);

        jrdd.takeOrdered(3).forEach(a-> System.out.println(a));

        Integer fold = jrdd.fold(0, (a, b) -> a + b);

        Integer aggregate = jrdd.aggregate(0, (a, b) -> a + b, (a, b) -> a + b);

        JavaPairRDD<Integer, Integer> pairRDD = jrdd.mapToPair(a -> new Tuple2<>(a, 1));

        Map<Integer, Long> integerLongMap = pairRDD.countByKey();

      /*  jrdd.saveAsObjectFile("");
        jrdd.saveAsTextFile("");


        F <: NewOutputFormat[_, _]](
                path: String,
                keyClass: Class[_],
                valueClass: Class[_],
                outputFormatClass: Class[F]) {
            rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass
        pairRDD.saveAsNewAPIHadoopFile();*/


        jrdd.foreachPartition(a-> System.out.println(a));
        jsc.stop();
    }
}
