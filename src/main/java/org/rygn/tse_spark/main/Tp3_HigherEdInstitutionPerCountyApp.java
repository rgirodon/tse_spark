package org.rygn.tse_spark.main;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.element_at;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Tp3_HigherEdInstitutionPerCountyApp {

	/**
	   * main() is your entry point to the application.
	   * 
	   * @param args
	   */
	  public static void main(String[] args) {
		  
		  Tp3_HigherEdInstitutionPerCountyApp app = new Tp3_HigherEdInstitutionPerCountyApp();
	    
		  app.start();
	  }

	  /**
	   * The processing code.
	   */
	  private void start() {
	    // Creation of the session
	    SparkSession spark = SparkSession
	    						.builder()
						        .appName("Tp3_HigherEdInstitutionPerCountyApp")
						        .master("local")
						        .getOrCreate();

	    // Ingestion of the census data
	    Dataset<Row> censusDf = spark
	    							.read()
							        .format("csv")
							        .option("header", "true")
							        .option("inferSchema", "true")
							        .option("encoding", "cp1252")
							        .load("data/census/PEP_2017_PEPANNRES.csv");
	    
	    censusDf = censusDf
			        .drop("GEO.id")
			        .drop("rescen42010")
			        .drop("resbase42010")
			        .drop("respop72010")
			        .drop("respop72011")
			        .drop("respop72012")
			        .drop("respop72013")
			        .drop("respop72014")
			        .drop("respop72015")
			        .drop("respop72016")
			        .withColumnRenamed("respop72017", "pop2017")
			        .withColumnRenamed("GEO.id2", "countyId")
			        .withColumnRenamed("GEO.display-label", "county");
	    	   	    
	    censusDf.show();
	    
	    
	    // Higher education institution
	    Dataset<Row> higherEdDf = spark
							        .read()
							        .format("csv")
							        .option("header", "true")
							        .option("inferSchema", "true")
							        .load("data/dapip/InstitutionCampus.csv");
	    
	    higherEdDf = higherEdDf
				        .filter("LocationType = 'Institution'");
	    
	    higherEdDf = higherEdDf
				        .withColumn(
				            "addressElements",
				            split(higherEdDf.col("Address"), " "));
	    
	    higherEdDf = higherEdDf
				        .withColumn(
				            "addressElementCount",
				            size(higherEdDf.col("addressElements")));
	    
	    higherEdDf = higherEdDf
				        .withColumn(
				            "zip9",
				            element_at(
				                higherEdDf.col("addressElements"),
				                higherEdDf.col("addressElementCount")));
	    
	    higherEdDf = higherEdDf
				        .withColumn(
				            "splitZipCode",
				            split(higherEdDf.col("zip9"), "-"));
	    
	    higherEdDf = higherEdDf
				        .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
				        .withColumnRenamed("LocationName", "location")
				        .drop("DapipId")
				        .drop("OpeId")
				        .drop("ParentName")
				        .drop("ParentDapipId")
				        .drop("LocationType")
				        .drop("Address")
				        .drop("GeneralPhone")
				        .drop("AdminName")
				        .drop("AdminPhone")
				        .drop("AdminEmail")
				        .drop("Fax")
				        .drop("UpdateDate")
				        .drop("zip9")
				        .drop("addressElements")
				        .drop("addressElementCount")
				        .drop("splitZipCode");
	    	    
	    higherEdDf.show();
	    
	    // Zip to county
	    Dataset<Row> countyZipDf = spark
							        .read()
							        .format("csv")
							        .option("header", "true")
							        .option("inferSchema", "true")
							        .load("data/hud/COUNTY_ZIP_092018.csv");
	    
	    countyZipDf = countyZipDf
				        .drop("res_ratio")
				        .drop("bus_ratio")
				        .drop("oth_ratio")
				        .drop("tot_ratio");

	    countyZipDf.show();
	    
	    
	    // Institutions per county id
	    Dataset<Row> institPerCountyDf = higherEdDf.join(
												        countyZipDf,
												        higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
												        "inner");	    
	    
	    // Institutions per county name
	    institPerCountyDf = institPerCountyDf.join(
										        censusDf,
										        institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
										        "left");
	    
	    // Final clean up
	    institPerCountyDf = institPerCountyDf
						        .drop(higherEdDf.col("zip"))
						        .drop(countyZipDf.col("county"))
						        .drop("countyId")
						        .distinct();

	    System.out.println("Final list");
	    institPerCountyDf.show(200, false);
	    
	    System.out.println("The combined list has " + institPerCountyDf.count()
	        + " elements.");
	    
	    
	  }
}
