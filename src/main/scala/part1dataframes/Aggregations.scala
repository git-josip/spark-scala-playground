package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val generesCountDF = moviesDF.select(count(col("Major_Genre")))
  val generesCountAllDF = moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating"))).show()
  moviesDF.select(sum(col("IMDB_Rating"))).show()
  moviesDF.select(avg(col("IMDB_Rating"))).show()
  moviesDF.select(mean(col("IMDB_Rating"))).show()
  moviesDF.select(stddev(col("IMDB_Rating"))).show()

  // grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")).count().show()

  moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating").show()

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */


  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  moviesDF
    .select(col("US_Gross"))
    .agg(
      mean(col("US_Gross")),
      stddev(col("US_Gross"))
    )
    .show()

  moviesDF
    .select(col("Director"), col("US_Gross"), col("IMDB_Rating"))
    .groupBy(col("Director"))
    .agg(
      avg(col("US_Gross")),
      avg(col("IMDB_Rating"))
    )
    .show()
}
