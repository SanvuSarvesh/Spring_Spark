package com.example.SparkInSpringBoot;

import org.apache.spark.sql.*;

public class SparkInSpringBoot {
	public static void main(String[] args) throws Exception {
		// Creating Spark session
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("testSpark")
				.master("local")
				.config("spark.driver.host","localhost")
				.getOrCreate();

		// Read data from any file .csv
		// Creating dataset from .csv file
		Dataset<Row> dataset1 = sparkSession
				.read()
				.option("header","true")
				.csv("C:\\Users\\Sarvesh Kumar\\Downloads\\practice.csv");
		//here address of the file
		dataset1.show();

		dataset1.createOrReplaceTempView("dataset1");
		Dataset<Row> rowNumber = sparkSession.sql("select * ," +
				" row_number() over(order by department) as rowNumber from dataset1");
		rowNumber.show();
//		// creating dataset from list of string,Integer etc..
//		Dataset<String > dataset2 = sparkSession.createDataset(Arrays
//				.asList("Sanvu","Sarvesh","Ritu","Bihar","Something"),Encoders.STRING());
//		dataset2.show();

		// Creating dataset from list of row spark.sql
//		List<Row> rowList = new ArrayList<>();
//		Row row = RowFactory.create("Sanvu","Albnero");
//		rowList.add(row);
//		Row row1 = RowFactory.create("Bihar","India");
//		rowList.add(row1);
//		Dataset<Row> rowDataset = sparkSession.createDataset(rowList,getEncode());
//		rowDataset.show();

//		Creating dataset from existing data source form transformation
//		@SuppressWarnings("unchecked")
//		Dataset<Row> transformDataset = rowDataset
//				.mapPartitions(new );

	}

//	private static ExpressionEncoder<Row> getEncode(){
//		StructType structType = new StructType();
//		structType = structType.add("name", DataTypes.StringType,false);
//		structType = structType.add("company",DataTypes.StringType,false);
//		return RowEncoder.apply(structType);
//	}

	/*
	*
	* 	Window function in spark :-
	* 		Spark window functions operates on group of row and return single value
	* 		Types of window functions
	* 			1. Ranking window function
	* 			2. Analytical window function
	* 			3. Aggregate window function
	*
	* */
}
