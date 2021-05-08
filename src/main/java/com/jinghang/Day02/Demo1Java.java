package com.jinghang.Day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Demo1Java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo1Java").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rddint = jsc.parallelize(list);
        int numPartitions = rddint.getNumPartitions();
        System.out.println(numPartitions);
        JavaRDD<Integer> rddint2 = jsc.parallelize(list, 5);
        numPartitions=rddint2.getNumPartitions();
        System.out.println(numPartitions);

        JavaPairRDD<Integer, Integer> rddpair1 = rddint2.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        });

        List<Tuple2<Integer, Integer>> tuple2s = Arrays.asList(new Tuple2<>(5, 5), new Tuple2<>(6, 5), new Tuple2<>(9, 5));

        JavaRDD<Tuple2<Integer, Integer>> rddpair2 = jsc.parallelize(tuple2s);

        jsc.close();
    }
}
