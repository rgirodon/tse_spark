package org.rygn.tse_spark.main;

import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Transforming records.
 *
 * @author rygn
 */
public class TP1_SQL {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        TP1_SQL app = new TP1_SQL();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creation of the session
        SparkSession spark = SparkSession.builder()
                .appName("Record transformations")
                .master("local")
                .getOrCreate();

        // Ingestion of the census data
        Dataset<Row> intermediateDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/census/PEP_2017_PEPANNRES.csv");

        Dataset<Row> finaldf = null;

        // Renaming and dropping the columns we do not need
        intermediateDf.createOrReplaceTempView("PEP_2017_VIEW");

        intermediateDf = spark.sql("SELECT substring(`GEO.id2`,1,2) as stateid, substring(`GEO.id2`,3,5) as countyid, split(`GEO.display-label`, ',')[0] as state, split(`GEO.display-label`, ',')[1] as county, rescen42010 as real2010 ,respop72010 as est2010, respop72017 as est2017 " + "FROM PEP_2017_VIEW "
                + "ORDER BY stateid, countyid");

        intermediateDf.show();

        intermediateDf.createOrReplaceTempView("FINAL_VIEW");

        finaldf = spark.sql("SELECT *, est2010 - real2010 as diff, est2017 - est2010 as growth " + "FROM FINAL_VIEW ");

        finaldf.show();

    }
}
