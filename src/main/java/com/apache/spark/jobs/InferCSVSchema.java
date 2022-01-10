package com.apache.spark.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {

    public void printSchema() {
        SparkSession spark = SparkSession.builder().appName("myApp").master("local").getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header",true)
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                .option("inferSchma", true)
                .load("src/main/resources/amazonProducts.txt");

        System.out.println("Excerpts of the dataframe content:");
        df.show(7, 90);
        System.out.println("Data Frames Schema: ");
        df.printSchema();
    }
}
