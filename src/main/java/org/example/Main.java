package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();

        Dataset<Row> csv = sparkSession.read().option("header", true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        //csv.show(false);



        csv = csv.filter(functions.col("Receiving Country Code").isin("LV", "MK", "MT"));
        csv.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy(new Column("Receiving Country Code"))
                .show(40);



        String url = "jdbc:mysql://localhost:3306/Country";
        String user = "root";
        String password = "Alexandru";
        String table_LV="Lithuania";
        String table_MK="Makedonia";
        String table_MT="Montenegro";
        String table_3Countries = "Countries";


        Dataset<Row> data_countries = sparkSession.read().option("header", true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        data_countries = data_countries.filter(functions.col("Receiving Country Code").isin("LV","MK","MT"));

        data_countries = data_countries.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code"));
        data_countries.show();

        data_countries
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("dbtable",table_3Countries)
                .option("url",url)
                .option("user",user)
                .option("password",password)
                .save();
        //First Country:LV

        Dataset<Row> data_LV = sparkSession.read().option("header", true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        data_LV = data_LV.filter(functions.col("Receiving Country Code").equalTo("LV"));

        data_LV = data_LV.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code"));
        data_LV.show();



        data_LV .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("url",url)
                .option("dbtable",table_LV)
                .option("user",user)
                .option("password",password)
                .save();

        //Second Country:MK

        Dataset<Row> data_MK = sparkSession.read().option("header", true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        data_MK = data_MK.filter(functions.col("Receiving Country Code").equalTo("MK"));

        data_MK = data_MK.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code"));
        data_MK.show();



        data_MK .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("url",url)
                .option("dbtable",table_MK)
                .option("user",user)
                .option("password",password)
                .save();

        //Third Country:MT

        Dataset<Row> data_MT = sparkSession.read().option("header", true).csv("/Users/alexandrucampan/MyCodes/Spark/src/main/resources/erasmus.csv");

        data_MT = data_MT.filter(functions.col("Receiving Country Code").equalTo("MT"));

        data_MT = data_MT.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code"));
        data_MT.show();

        data_MT .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("url",url)
                .option("dbtable",table_MT)
                .option("user",user)
                .option("password",password)
                .save();
    }
}