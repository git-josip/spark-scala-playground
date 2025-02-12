package dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))


  val carsDFSchema = spark.read
    .format("json")
    .schema(carsSchema) // enforce schema
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .option("dateFormat", "yyyy-MM-dd")
    .load()

  carsDFSchema.show()


  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val xxx = spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")
  xxx.show()

  // Parquet
  carsDFSchema.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // text files
  spark.read.text("src/main/resources/data/sample_text.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/sparkplayground"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()


  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */


  val moviesJsonDFSchema = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/movies.json")
    .option("dateFormat", "dd-MMM-yy")
    .load()

  // csv tab
  moviesJsonDFSchema.write
    .mode(SaveMode.Overwrite)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("sep", "  ")
    .option("nullValue", "")
    .csv("src/main/resources/data/movies.csv")

  moviesJsonDFSchema.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  moviesJsonDFSchema.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
