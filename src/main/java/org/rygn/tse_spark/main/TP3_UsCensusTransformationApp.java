package org.rygn.tse_spark.main;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Transforming records.
 * 
 * @author rygn
 */
public class TP3_UsCensusTransformationApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    TP3_UsCensusTransformationApp app = new TP3_UsCensusTransformationApp();
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

    // Renaming and dropping the columns we do not need
    
    // drop columns GEO.id, resbase42010, respop72011, respop72012, respop72013, respop72014, respop72015, respop72016 (tip : dataset drop method)
    intermediateDf = intermediateDf
            			.drop("GEO.id")
            			.drop("resbase42010")
            			.drop("respop72011")
            			.drop("respop72012")
            			.drop("respop72013")
            			.drop("respop72014")
            			.drop("respop72015")
            			.drop("respop72016");
            			    
    // rename column GEO.id2 -> id, GEO.display-label -> label, rescen42010 -> real2010, respop72010 -> est2010, respop72017 -> est2017 (tip : dataset withColumnRenamed method)
    intermediateDf = intermediateDf
    					.withColumnRenamed("GEO.id2", "id")
    					.withColumnRenamed("GEO.display-label", "label")
    					.withColumnRenamed("rescen42010", "real2010")
    					.withColumnRenamed("respop72010", "est2010")
    					.withColumnRenamed("respop72017", "est2017");
    
    	
    
    
    // Creates the additional columns
    
    // create column countyState as an array from column label splitted around the "," (tip : dataset withColumn and col methods, and split function)
    intermediateDf = intermediateDf
						.withColumn("splittedLabel", split(intermediateDf.col("label"), ", "));
    
    // create column state as 2nd part of column countyState (tip : column getItem method)
    intermediateDf = intermediateDf
    					.withColumn("state", intermediateDf.col("splittedLabel").getItem(1));
    
    // create column county as 1st part of column countyState (tip : column getItem method)
    intermediateDf = intermediateDf
						.withColumn("county", intermediateDf.col("splittedLabel").getItem(0));
    
    intermediateDf = intermediateDf
						.drop("splittedLabel")
						.drop("label");
    
    
    // create column stateId from column id (tip : expr function)
    intermediateDf = intermediateDf
    					.withColumn("stateId", expr("int(id / 1000)"));
    
    // create column countyId from column id (tip : expr function)
    intermediateDf = intermediateDf
						.withColumn("countyId", expr("id % 1000"));
        
    intermediateDf = intermediateDf
    					.drop("id");
    
    
    // Compute new columns

    // diff is est2010 - real2010 (tip : dataset withColumn method, and expr function)
    intermediateDf = intermediateDf
						.withColumn("diff", expr("est2010 - real2010"));
    
    // growth is est2017-est2010 (tip : dataset withColumn method, and expr function)
    intermediateDf = intermediateDf
						.withColumn("growth", expr("100.0*(est2017 - est2010) / est2010"));
    
    intermediateDf = intermediateDf
						.drop("est2010")
						.drop("real2010")
						.drop("est2017")
						.drop("real2017");
    
    intermediateDf = intermediateDf.select("stateId", "countyId", "state", "county", "diff", "growth");
    
    intermediateDf.show();	
    
  }
}
