package com.jinghang.Day11;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.In;
import scala.Tuple3;

public class Demo2JsonJ {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo2JsonJ").setMaster("local[*]");

        SparkSession ss = SparkSession.builder().config(conf)
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaRDD<String> javaRDD = jsc.textFile("E:\\jinghang\\课件03\\day18\\json_data\\test.json");

        javaRDD.map(new Function<String, Tuple3<Integer,Integer,String>>() {
            @Override
            public Tuple3 call(String v1) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(v1);
                int movie = jsonNode.get("movie").asInt();
                int rate = jsonNode.get("rate").asInt();
                String name="";
                if (jsonNode.has("name")) {
                    name=jsonNode.get("name").asText();
                }

                return new Tuple3(movie,rate,name);
            }
        }).foreach(a-> System.out.println(a));
        ss.close();
        jsc.close();
    }
}
