package org.rygn.tse_spark.main;

import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TP3_SQL {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {

        TP3_SQL app = new TP3_SQL();

        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creation of the session
        SparkSession spark = SparkSession
                .builder()
                .appName("TP3_macle")
                .master("local")
                .getOrCreate();

        Dataset<Row> censusDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/census/PEP_2017_PEPANNRES.csv");

        Dataset<Row> higherEdDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/dapip/InstitutionCampus.csv");

        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/hud/COUNTY_ZIP_092018.csv");

        Dataset<Row> join = null;

        censusDf.createOrReplaceTempView("censusDf");
        higherEdDf.createOrReplaceTempView("higherEdDf");
        countyZipDf.createOrReplaceTempView("countyZipDf");
        join = spark.sql("Select countyZipDf.`zip` ,higherEdDf.`LocationName`, countyZipDf.`county`, censusDf.`respop72017` " +
                "FROM censusDf " +
                "join countyZipDf on censusDf.`Geo.id2` = countyZipDf.county " +
                "join higherEdDf on countyZipDf.zip = SUBSTRING_INDEX(higherEdDf.address, ' ', -1) " +
                " order by countyZipDf.`zip` asc");
        join.show();
    }
}
