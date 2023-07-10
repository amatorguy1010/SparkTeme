package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;


public class Main {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();

        Dataset<Row> csv = sparkSession.read().option("header",true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        csv.show(false);

        csv.groupBy("Receiving Country Code","Sending Country Code")
                    .count()
                    .orderBy(new Column("Receiving Country Code"))
                    .show(40);
    }
}