package org.rygn.tse_spark.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV to a relational database.
 * 
 * @author rygn
 */
public class CsvToRelationalDatabaseSparkSqlApp {

	/**
	 * main() is your entry point to the application.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		// Creates a session on a local master
		SparkSession spark = SparkSession.builder().appName("CSV to DB").master("local").getOrCreate();

		// Step 1: Ingestion
		// ---------

		// Reads a CSV file with header, called authors.csv, stores it in a
		// dataframe
		Dataset<Row> df = spark.read().format("csv").option("header", "true").load("data/authors.csv");

		df.createOrReplaceTempView("authors_view");

		// Step 2: Transform
		// ---------

		// Creates a new column called "name" as the concatenation of lname, a
		// virtual column containing ", " and the fname column
		df = spark.sql("SELECT fname, lname, CONCAT(fname, ' ',  UPPER(lname)) AS full_name " + "FROM authors_view "
				+ "ORDER BY lname, fname");

		df.show();

		df.write().option("header", true).csv("data/output/authors_sql");

		/*
		 * // Step 3: Save // ----
		 * 
		 * // The connection URL, assuming your PostgreSQL instance runs locally on //
		 * the // default port, and the database we use is "spark_labs" String
		 * dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs";
		 * 
		 * // Properties to connect to the database, the JDBC driver is part of our //
		 * pom.xml Properties prop = new Properties(); prop.setProperty("driver",
		 * "org.postgresql.Driver"); prop.setProperty("user", "postgres");
		 * prop.setProperty("password", "RafaelYanice10");
		 * 
		 * // Write in a table called ch02 df.write() .mode(SaveMode.Overwrite)
		 * .jdbc(dbConnectionUrl, "ch03", prop);
		 */

		spark.stop();

		System.out.println("Process complete");
	}

}
