package com.jinghang.Day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.collection.mutable.ListBuffer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ProvinceADJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ProvinceADJava").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> jRdd1 = jsc.textFile("E:\\jinghang\\课件03\\day10\\agent.log");
        JavaRDD<String> jRdd2 = jsc.textFile("E:\\jinghang\\课件03\\day10\\agent1.log");


        JavaPairRDD<Tuple2<String, String>, Integer> pairRDD = jRdd1.mapToPair(a -> new Tuple2(new Tuple2<>(a.split(" ")[1], a.split(" ")[4]), 1));
        JavaPairRDD<Tuple2<String, String>, Integer> byKey = pairRDD.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String, Tuple2<String, Integer>> pairRDD1 = byKey.mapToPair(a -> new Tuple2<>(a._1._1, new Tuple2<>(a._1._2, a._2)));
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> pairRDD2 = pairRDD1.groupByKey();
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> pairRDD3 = pairRDD2.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> v1) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                Iterator<Tuple2<String, Integer>> it = v1.iterator();
                while (it.hasNext()) {
                    list.add(it.next());
                }
                list.sort(new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return o2._2 - o1._2;
                    }
                });
                ;
                return list.subList(0, 3);
            }
        });/* .sortWith(_._2 > _._2).take(3))*/
        pairRDD3.foreach(a-> System.out.println(a));
        System.out.println("---------------------------------");
        JavaPairRDD<String,String> pairRDD4 = jRdd2.mapToPair(a -> new Tuple2(a.split(" ")[0], a.split(" ")[1]));
        pairRDD3.leftOuterJoin(pairRDD4).mapToPair(a->new Tuple2(a._2._2.get(),a._2._1)).foreach(a-> System.out.println(a));
    }


}
