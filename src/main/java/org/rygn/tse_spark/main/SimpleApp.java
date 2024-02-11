package org.rygn.tse_spark.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {

	public static void main(String[] args) {
		
		String logFile = "data/firstnames.txt";
		
		SparkSession spark = SparkSession.builder()
											.appName("Simple Application")
											.master("local")
											.getOrCreate();
		
		Dataset<String> logData = spark.read().textFile(logFile).cache();

		long numAs = logData.filter((String s) -> s.contains("a")).count();
		
		long numBs = logData.filter((String s) -> s.contains("b")).count();

		System.out.println("Firstnames with a: " + numAs + ", firstnames with b: " + numBs);

		spark.stop();
	}
}
