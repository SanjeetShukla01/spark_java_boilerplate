package com.apache.spark.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonLineParser {
    public void parseJsonLines() {
        SparkSession spark = SparkSession.builder().appName("myApp").master("local").getOrCreate();

        Dataset<Row> df = spark.read()
                .format("json")
                .load("src/main/resources/simple.json");

        Dataset<Row> df1 = spark.read()
                .format("json")
                .option("multiline", true)
                .load("src/main/resources/multiline.json");

        df.show();
        df.printSchema();

        df1.show();
        df1.printSchema();

    }
}
