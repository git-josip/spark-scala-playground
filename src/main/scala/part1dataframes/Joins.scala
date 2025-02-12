package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  guitaristsDF.join(bandsDF, joinCondition, "left_outer")
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // ex
  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */


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

  val employeeSalariesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()

  val departmenMangersDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.titles")
    .load()

  // 1
  val employeeWithSalaries = employeesDF.join(employeeSalariesDF.withColumnRenamed("emp_no", "sal_emp_no"), employeesDF.col("emp_no") === col("sal_emp_no"), "left_outer")
  employeeWithSalaries.select(
      col("emp_no"),
      col("salary")
    ).groupBy(col("emp_no"))
    .agg(max(col("salary")).as("max_salary"))
    //    .orderBy(col("max_salary"))
    .show()

  // 2
  val notManagerEmpDF = employeesDF.join(departmenMangersDF, employeesDF.col("emp_no") === departmenMangersDF.col("emp_no"), "left_anti")
  notManagerEmpDF.show()

  // 3
  val employeeWithSalaries2DF = employeesDF.join(employeeSalariesDF.withColumnRenamed("emp_no", "sal_emp_no"), employeesDF.col("emp_no") === col("sal_emp_no"))
  val employeesSalariesWithTitle = employeeWithSalaries2DF.join(titlesDF.withColumnRenamed("emp_no", "title_emp_no"), employeeWithSalaries2DF.col("emp_no") === col("title_emp_no"))

  employeesSalariesWithTitle.select(
    col("emp_no"),
    col("title"),
    col("salary")
  ).groupBy(col("emp_no"))
    .agg(
      max(col("salary")).as("max_salary")
    )
    .orderBy(col("max_salary").desc)
    .limit(10)
    .show()


}

