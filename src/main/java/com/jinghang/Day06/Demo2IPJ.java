package com.jinghang.Day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Demo2IPJ {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo2IPJ").setMaster("local[*]");
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Tuple3<Long, Long, String>> list = jsc.textFile("E:\\jinghang\\课件03\\day13\\ip.txt").map(i ->
                new Tuple3<Long, Long, String>(Long.parseLong(i.split("\\|")[2]), Long.parseLong(i.split("\\|")[3]), i.split("\\|")[6])
        ).collect();

        Broadcast<List<Tuple3<Long, Long, String>>> bc = jsc.broadcast(list);

        jsc.textFile("E:\\jinghang\\课件03\\day13\\20090121000132.394251.txt").mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] ipa =s.split("\\|")[1].split("\\.");

                long ipl = Long.parseLong(ipa[0]) * 256 * 256 * 256 + Long.parseLong(ipa[1]) * 256 * 256 + Long.parseLong(ipa[2]) * 256 + Long.parseLong(ipa[3]);
                String add = mysearch(bc.value(), ipl);
                return new Tuple2<>(add,1);
            }
        }).reduceByKey((a,b)->a+b).takeOrdered(3, new mycomparator())
        .forEach(a-> System.out.println(a));
    }

    private static String mysearch(List<Tuple3<Long, Long, String>> value, long ipl) {
        Integer start=0;
        Integer end=value.size()-1;
        Integer mid=0;
        while (start<=end){
            mid=(start+end)/2;
            if (ipl>=value.get(mid)._1()&& ipl<=value.get(mid)._2()){
                return value.get(mid)._3();
            }else if (ipl>value.get(mid)._2()){
                start=mid+1;
            }else if (ipl < value.get(mid)._1()){
                end=mid-1;
            }
        }

        return "未知地点";
    }
    static class mycomparator<T extends Tuple2<String,Integer>>implements Serializable,Comparator<T>{


        @Override
        public int compare(T o1, T o2) {
            return o2._2-o1._2;
        }
    }

}
