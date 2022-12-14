//package org.example
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{column, element_at, explode, lit, regexp_replace, split}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession


object TNPlantation{
  lazy val sparkconf: SparkConf = new SparkConf()
    .setAppName("TN plantation")
    .setMaster("local[2]")
    .set("spark.cores.max", "2")
    .set("spark.executor.memory", "1g")

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("TN_Plantation")
    .config("spark.master", "local")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]) {

    val bamboo_path = "E:/Chaithra_Learning/Spark/bamboo-plantation-15-16.txt"
    val tea_path = "E:/Chaithra_Learning/Spark/tea-plantation-15-16.txt"
    val rubber_path = "E:/Chaithra_Learning/Spark/rubber-plantation-15-16.txt"


    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Cleaning Bamboo Dataframe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    val b_df = clean_bamboo(bamboo_path)
    b_df.show()
    b_df.printSchema()

    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Tea Dataframe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    val t_df = clean_tea(tea_path)
    b_df.show()
    t_df.printSchema()

    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Rubber Dataframe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    val r_df = clean_rubber(rubber_path)
    b_df.show()
    r_df.printSchema()

    println(">>>>>>>>>>>>>>>>>>>>>> Union of Bamboo, Tea, Rubber dataframe <<<<<<<<<<<<<<<<<<,")
    val all_df = b_df.union(t_df).union(r_df)
    all_df.show()
    all_df.printSchema()

    all_df.createOrReplaceTempView("tn_plantation_df")

    // Problem1 - Analyse which of the district have maximum and least production of bamboo, tea, rubber
    println(">>>>>>>>>>>>>>>>>>> District with Least & Maximum production of bamboo, tea, rubber <<<<<<<<<<<<<<")
    val res01 = spark.sql(
          "WITH CTE AS (SELECT *,DENSE_RANK() OVER(PARTITION BY `Plantation Type` ORDER BY Production DESC) as Max_Prod_rank," +
            "DENSE_RANK() OVER(PARTITION BY `Plantation Type` ORDER BY Production) as Min_Prod_rank FROM tn_plantation_df)" +
            "SELECT `Plantation Type`,Remarks,District, Production FROM (" +
            "SELECT *, " +
            "CASE WHEN Max_Prod_rank = 1 THEN CONCAT('Maximum ',`Plantation Type`,' producing district') " +
            "WHEN Min_Prod_rank = 2 THEN CONCAT('Least ',`Plantation Type`,' producing district') " +
            "ELSE 'na' END as Remarks" +
            " FROM CTE) WHERE Remarks <> 'na';")

      res01.show()

    //Problem2 - Total area of each plantation under all districts
    println(">>>>>>>>>>>>>>>>>> Total area of each plantation under all districts <<<<<<<<<<<<<<<<")
    val res02 = spark.sql("SELECT `Plantation Type`, SUM(Area) as Total_Area_of_Plantation FROM tn_plantation_df GROUP BY `Plantation Type`;")
    res02.show()


    b_df.createOrReplaceTempView("bamboo")
    t_df.createOrReplaceTempView("tea")
    r_df.createOrReplaceTempView("rubber")
    println(">>>>>>>>>>>>>>>>>>> Showing Relationship of bamboo, tea and rubber plantation for each of the district <<<<<<<<<<<<<<<<<<")
    val res03 = spark.sql("SELECT b.District as DISTRICT,b.Productivity as BAMBOO_PRODUCTIVITY, t.Productivity as TEA_PRODUCTIVITY,r.Productivity as RUBBER_PRODUCTIVITY" +
      " FROM bamboo b INNER JOIN tea t ON b.Serial_Number = t.Serial_Number INNER JOIN rubber r ON b.Serial_Number = r.Serial_Number")
    res03.show()
  }

//********************************************************** Bamboo Plantation data cleaning ********************************************************
  def clean_bamboo(src_path:String):DataFrame={
    //Manually defining t_df schema
    val schema = StructType(Array(
      StructField("Serial_Number", IntegerType, true),
      StructField("District", StringType, true),
      StructField("Area", StringType, true),
      StructField("Production", IntegerType, true)))

    //Loading bamboo file into t_df by referring to manually defined schema
    var b_df = spark.read
      .format(source = "com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "false")
      .option("inferSchema", "false")
      .schema(schema)
      .load(path = src_path).toDF()

    /*Modifying existing column data & Adding new columns to bamboo t_df
  1. Removing special characters from district col & alphabets from area col
  2. Removing null from below columns & replacing them with 0(Int) or 0.0(Float)*/
    b_df = b_df.withColumn("District", functions.regexp_replace(column("District"), "[^a-zA-Z0-9]", ""))
      .withColumn("Area", functions.regexp_replace(column("Area"), "[^0-9]", "").cast(IntegerType)).na.fill(0, Array("Area", "Production"))
      .withColumn("Plantation Type", lit(extract_type(src_path)))
      .withColumn("Productivity", (column("Production") / column("Area")).cast(FloatType)).na.fill(0.0, Array("Productivity"))

    return b_df
  }

//********************************************************** Tea plantation data cleaning ********************************************************
  def clean_tea(src_path:String):DataFrame={
    //Reading text file into Dataframe (text file: all | separated data in the same file)
    var t_df = spark.read.text(src_path)

    //Replacing 5th delimiter by \n
    t_df = t_df.withColumn("value", regexp_replace(t_df.col("value"), s"((?:[^\\|]*\\|){4}[^\\|]*)\\|", "$1\n"))

    /*
    splitting line by \n --> array with each row as ele --> then each ele of an array is exploded into different rows
   --> each row data which is in string is split by | to get array of 5 ele ===> finally to fetch col of array of strings(data) as each row
   */
    t_df = t_df.select(explode(split(t_df.col("value"), "\n"))).select(split(column("col"), "\\|").alias("value"))

    //Assigning ele of array to different column & dropping original column with array
    t_df = t_df.withColumn("Serial_Number", element_at(column("value"), 1).cast(IntegerType))
      .withColumn("District", element_at(column("value"), 2).cast(StringType))
      .withColumn("Area", element_at(column("value"), 3).cast(IntegerType)).na.fill(0)
      .withColumn("Production", element_at(column("value"), 4).cast(IntegerType)).na.fill(0)
      .withColumn("Plantation Type", lit(extract_type(src_path)))
      .withColumn("Productivity", element_at(column("value"), 5).cast(FloatType)).na.fill(0.0)
      .drop("value")

    return t_df
  }

//********************************************************** Rubber plantation data cleaning ********************************************************
  def clean_rubber(src_path:String):DataFrame={
    var r_df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(src_path)

    r_df = r_df.withColumn("Serial_Number", $"Serial_Number".cast(IntegerType))
      .withColumn("District", $"District".cast(StringType))
      .withColumn("Area", $"Area".cast(IntegerType))
      .withColumn("Production", $"Production".cast(IntegerType))
      .withColumn("Plantation Type", lit(extract_type(src_path)))
      .withColumn("Productivity", $"Production" / $"Area".cast(FloatType)).na.fill(0.0, Array("Productivity")).na.fill(0, Array("Area", "Production"))

    return r_df
  }

  def extract_type(fpath: String): String = {
    val f_name = fpath.split("/")(3)
    return f_name.substring(0, f_name.indexOf("-"))
  }

}




