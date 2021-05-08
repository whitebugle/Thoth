package com.jinghang.Day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Demo2Java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo2Java").setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //map(jsc);
        //mappartition(jsc);
        //mappartitionwithindex(jsc);

        //glom(jsc);
        //flatmap(jsc);
        //filter(jsc);
        //sample(jsc);
        groupbykey(jsc);
        jsc.close();
    }

    public static void map(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer*10;
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }

    public static void mappartition(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                ArrayList<Integer> list = new ArrayList<>();
                while (integerIterator.hasNext()) {
                   list.add(integerIterator.next()*10) ;
                }
                return list.iterator();
            }
        },false).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    public static void mappartitionwithindex(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> integerIterator) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                while (integerIterator.hasNext()) {
                   list.add(index+":"+ integerIterator.next());
                }
                return list.iterator();
            }
        },false).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    public static void flatmap(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaPairRDD<Integer, Iterable<Integer>> grouprdd = rdd.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer % 2;
            }
        });
            grouprdd.foreach(a-> System.out.println(a));
        System.out.println("---------------------------------");

        grouprdd.flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Integer>>, Integer>() {
            @Override
            public Iterator<Integer> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                return integerIterableTuple2._2.iterator();
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }


    //将一个分区的数据转化为一个数组
    public static void glom(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<List<Integer>> glomrdd = rdd.glom();
        glomrdd.foreach(new VoidFunction<List<Integer>>() {
            @Override
            public void call(List<Integer> integers) throws Exception {
                System.out.println(integers);
            }
        });
    }

    public static void filter(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> filterdd = rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });
        filterdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    public static void sample(JavaSparkContext jsc){
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> sample = rdd.sample(true, 0.5);
        sample.foreach(a-> System.out.println(a));
    }

    public static void groupbykey(JavaSparkContext jsc){
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("are u ok", "how are u", "how old are you", "how you you u how", "fine thank you"));
        JavaRDD<String> rdd1 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {

                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).groupByKey();
        rdd3.map(new Function<Tuple2<String, Iterable<Integer>>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                int sum=0;
                Iterator<Integer> it = stringIterableTuple2._2.iterator();
                while (it.hasNext()) {
                    sum+=it.next();
                }

                return new Tuple2<>(stringIterableTuple2._1,sum);
            }
        }).foreach(a-> System.out.println(a));
    }


}
