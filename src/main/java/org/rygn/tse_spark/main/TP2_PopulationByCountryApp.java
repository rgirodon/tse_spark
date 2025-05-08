package org.rygn.tse_spark.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.expr;

/**
 * Simple SQL select on ingested data after preparing the data with the
 * dataframe API.
 * 
 * @author jgp
 */
public class TP2_PopulationByCountryApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    TP2_PopulationByCountryApp app = new TP2_PopulationByCountryApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Create a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Simple SQL")
        .master("local")
        .getOrCreate();

    // Create the schema for the whole dataset
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "geo",
            DataTypes.StringType,
            true),
        DataTypes.createStructField(
            "yr1980",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1981",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1982",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1983",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1984",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1985",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1986",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1987",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1988",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1989",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1990",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1991",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1992",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1993",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1994",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1995",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1996",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1997",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1998",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr1999",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2000",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2001",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2002",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2003",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2004",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2005",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2006",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2007",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2008",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2009",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "yr2010",
            DataTypes.DoubleType,
            false) });

    // Reads a CSV file with header (as specified in the schema), called
    // populationbycountry19802010millions.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .schema(schema)
        .load("data/countrypop/populationbycountry19802010millions.csv");

    // Remove the columns we do not want (tip : for loop from 1981 to 2009)
    for (int i = 1981; i < 2010; i++) {
    	df = df.drop(df.col("yr" + i));
    }
    
    // Creates new column evolution as 100.0 * (yr2010 - yr1980) / yr1980 (tip : method withColumn of dataset and expr function)
    df = df.withColumn("evolution", expr("100.0 * (yr2010 - yr1980) / yr1980"));
        
    // Create a view geodata of the dataset 
    df.createOrReplaceTempView("geodata");
    
    // Keep only lines with negative evolution, ordered by evolution to have lowest evolution first
    Dataset<Row> negativeEvolutionDf =
            spark.sql(
                "SELECT * FROM geodata "
                    + "WHERE geo IS NOT NULL AND evolution <= 0 "
                    + "ORDER BY evolution ");

    // Shows 5 first rows
    negativeEvolutionDf.show(5);
    
	// Keep only lines with positive evolution, ordered by evolution desc to have highest evolution first
    Dataset<Row> positiveEvolutionDf =
            spark.sql(
                "SELECT * FROM geodata "
                    + "WHERE geo IS NOT NULL "
                    + "  AND evolution > 0 "
                    //+ "  AND geo NOT IN ('Asia', 'Africa', 'World', 'North America', 'Asia & Oceania', 'Central & South America', 'Middle East')"
                    + "ORDER BY evolution desc");

    // Shows 5 first rows
    positiveEvolutionDf.show(5);    
  }
}
