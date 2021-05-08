package com.jinghang.Day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;

public class UdfJ {
    static Long sum=0L;
    static Integer num=0;
    static Double av=0.0;
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Udfj").setMaster("local[*]");
        SparkSession ss = SparkSession.builder().config(conf)
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());

        Dataset<Row> json = ss.read().json("E:\\jinghang\\课件03\\people.json");
        json.createOrReplaceTempView("p");

        ss.udf().register("aav", new UDF1<Long, Double>() {
            @Override
            public Double call(Long lon) throws Exception {
                    sum+=lon;
                    num+=1;
                return sum.doubleValue()/num;
            }
        }, DataTypes.DoubleType);

        ss.sql("select name,aav(age) from p").show();
        jsc.close();
        ss.stop();


    }
}
