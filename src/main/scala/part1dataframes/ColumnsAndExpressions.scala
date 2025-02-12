package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val firstColumn = carsDF.col("Name")

  // selecting
  val carNamesDf = carsDF.select(firstColumn)
  carsDF.select(
    firstColumn,
    carsDF.col("Acceleration"),
    col("Acceleration"),
    expr("Origin")
  )

  val weightInLbsCol = carsDF.col("Weight_in_lbs")
  val weightInKgCol = carsDF.col("Weight_in_lbs") / 2.2

  carsDF.select(
    weightInLbsCol,
    weightInKgCol.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWithSelectExpr = carsDF
    .selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs / 2.2"
    )

  val carsWithKg3Column = carsDF
    .withColumn("Weight_in_kg_2", col("Weight_in_lbs") / 2.2)


  val moviesJsonDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val grossSumColumn = expr("US_Gross + Worldwide_Gross + US_DVD_Sales")

  val moviesProfitDF2 = moviesJsonDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  moviesJsonDF
    .select("Title", "Major_Genre", "IMDB_Rating")
    .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show()
}
