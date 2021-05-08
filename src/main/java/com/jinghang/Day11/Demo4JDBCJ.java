package com.jinghang.Day11;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class Demo4JDBCJ {
    public static void main(String[] args) throws ClassNotFoundException {
        SparkConf conf = new SparkConf().setAppName("Demo4JDBCJ").setMaster("local[*]");

        SparkSession ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        String url="jdbc:mysql://localhost:3306/db1?serverTimezone=UTC";
        String table="student";
        String driver="com.mysql.cj.jdbc.Driver";
        Properties prop = new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password","wu555555");
        Class.forName(driver);
        Dataset<Row> dsf = ss.read().jdbc(url, table, prop);
        dsf.show();

        ss.read().format("jdbc")
                .option("driver",driver)
                .option("url", url)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "wu555555")
                .load().show();

      /*  dsf.write().format("jdbc")
                .mode(SaveMode.Append)
                .option("","")
                .save();
        dsf.write().jdbc(url,table,prop);*/

        ss.close();
    }
}
