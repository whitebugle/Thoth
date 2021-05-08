package com.jinghang.Day03;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Demo1Java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo1Java").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

       // distinct(jsc);
        //repartition(jsc);
        partitionby(jsc);

        jsc.stop();
    }

    public static void distinct(JavaSparkContext jsc){
        List<String> list = Arrays.asList("are u ok", "how are u", "how old are you", "how you you u how", "fine thank you");
        JavaRDD<String> rdd = jsc.parallelize(list);
        JavaRDD<String> stringJavaRDD = rdd.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
        stringJavaRDD.distinct().foreach(a-> System.out.println(a));
        System.out.println(stringJavaRDD.distinct().count());

    }
    public static void repartition(JavaSparkContext jsc ){
        List<String> list = Arrays.asList("are u ok", "how are u", "how old are you", "how you you u how", "fine thank you");
        JavaRDD<String> javaRDD = jsc.parallelize(list, 3);

        System.out.println(javaRDD.repartition(5).getNumPartitions());
        System.out.println(javaRDD.repartition(2).getNumPartitions());

        System.out.println(javaRDD.coalesce(5).getNumPartitions());
        //coalesce 默认增多分区不发生shuffle ，需手动开启
        System.out.println(javaRDD.coalesce(5, true).getNumPartitions());

        System.out.println(javaRDD.coalesce(2).getNumPartitions());
    }

    public static void union(JavaSparkContext jsc){
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> javaRDD2 = jsc.parallelize(Arrays.asList(3, 4, 5, 6, 7));

        JavaRDD<Integer> union = javaRDD1.union(javaRDD2);
        union.foreach(a-> System.out.println(a));

        javaRDD1.subtract(javaRDD2).foreach(a-> System.out.println(a));

        javaRDD1.intersection(javaRDD2).foreach(a-> System.out.println(a));

        javaRDD1.cartesian(javaRDD2).foreach(a-> System.out.println(a));

        JavaPairRDD<Integer, Integer> zip = javaRDD1.zip(javaRDD2);
        zip.foreach(a-> System.out.println(a));
    }

    public static void partitionby(JavaSparkContext jsc){
        List<String> list = Arrays.asList("are u ok", "how are u", "how old are you", "how you you u how", "fine thank you");
        JavaRDD<String> javaRDD = jsc.parallelize(list, 3);
        JavaPairRDD pairRDD = javaRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator()).mapToPair(a -> new Tuple2(a, 1));

        System.out.println(pairRDD.partitionBy(new HashPartitioner(5)).getNumPartitions());
        System.out.println(pairRDD.partitionBy(new Mypartition(3)).getNumPartitions());
    }
    static class Mypartition extends Partitioner {
        private  Integer numpartition;

        Mypartition(Integer numpartition){
            this.numpartition=numpartition;
        }
        @Override
        public int numPartitions() {
            return numpartition;
        }

        @Override
        public int getPartition(Object key) {
            return key.hashCode()%numpartition;
        }
    }

    public static void aggregatebykey(JavaSparkContext jsc){
        List<String> list = Arrays.asList("are u ok", "how are u", "how old are you", "how you you u how", "fine thank you");
        JavaRDD<String> javaRDD = jsc.parallelize(list, 3);
        JavaPairRDD<String,Integer> pairRDD = javaRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator()).mapToPair(a -> new Tuple2(a, 1));

        pairRDD.reduceByKey((a , b ) -> a + b).foreach(a-> System.out.println(a));

        pairRDD.foldByKey(100,(a,b)->a+b);

        pairRDD.aggregateByKey(100,(a,b)->a+b,(a,b)->a+b).foreach(a-> System.out.println(a));

        pairRDD.combineByKey(i-> i+100 ,(a,b)->a+b,(a,b)->a+b);
    }

    public static void join(JavaSparkContext jsc){
        JavaPairRDD<String, Integer> rdd1 = jsc.parallelize(Arrays.asList("are", "u", "ok")).mapToPair(a -> new Tuple2<>(a, 1));
        JavaPairRDD<String, Integer> rdd2 = jsc.parallelize(Arrays.asList("are", "you", "ok")).mapToPair(a -> new Tuple2<>(a, 1));

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = rdd1.join(rdd2);
        join.foreach(a-> System.out.println(a));

        rdd1.leftOuterJoin(rdd2).foreach(a-> System.out.println(a));

        rdd1.rightOuterJoin(rdd2).foreach(a-> System.out.println(a));

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> outerJoin = rdd1.fullOuterJoin(rdd2);

        outerJoin.mapToPair(a->new Tuple2<>(a._1,new Tuple2<>(a._2._1==null?0:a._2._1,a._2._2==null?false:a._2._2))
        ).foreach(a-> System.out.println(a));
    }

    public static void cogroup(JavaSparkContext jsc){
        JavaPairRDD<Integer, String> rdd1 = jsc.parallelize(Arrays.asList(new Tuple2<>(1, "a"), new Tuple2<>(2, "b"), new Tuple2<>(3, "c"))).mapToPair(a->a);
        JavaPairRDD<Integer, Integer> rdd2 = jsc.parallelize(Arrays.asList(new Tuple2<>(1, 4), new Tuple2<>(2, 5), new Tuple2<>(3, 6))).mapToPair(a -> a);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = rdd1.cogroup(rdd2);
        cogroup.foreach(a-> System.out.println(a));
    }
}
