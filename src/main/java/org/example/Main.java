package org.example;

import org.apache.spark.sql.*;


public class Main {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();

        Dataset<Row> csv = sparkSession.read().option("header",true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        csv.show(false);

        csv = csv.filter(functions.col("Receiving Country Code").isin("LV","MK","MT"));
        csv.groupBy("Receiving Country Code","Sending Country Code")
                    .count()
                    .orderBy(new Column("Receiving Country Code"))
                    .show(40);
    }
}