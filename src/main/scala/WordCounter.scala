import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
  * Spark app to compute sum of words from multiple files and calculate rollups.
  * Not going by the way of rollup/cube in spark as the output of those commands are not in desired format.
  * This app can be extended to n number of files.
  */
object WordCounter {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession =  SparkSession.builder().appName("WordCounter").getOrCreate()
    val inputDF = spark.read.text(args : _*)
    val all_counts = transform(spark, inputDF)
    val formattedDF:DataFrame = formatDF(all_counts)
    formattedDF.collect().foreach(row=> println(row.getAs[String]("print_column")))
  }

  def transform(spark:SparkSession, inputDF: DataFrame): DataFrame  ={
    val get_file_name=spark.udf.register("get_file_name", (fullPath: String) => fullPath.split("/").last)

    import spark.implicits._

    //Calculate file name pivoted count of each word
    val fileWiseCounts = inputDF.select(get_file_name(input_file_name).as("file_name"), split($"value","\\W").as("values"))
      .select($"file_name",explode($"values").as("value"))
      .filter($"value"=!="")
      .groupBy("value")
      .pivot("file_name")
      .count()

    //Get all non word columns i.e. count columns required for producing final sum
    val countColumns = fileWiseCounts.columns.filter(!_.equals("value"))

    //Fill all nulls with 0 in coun columns
    val countsWithZeros = fileWiseCounts.na.fill(0,countColumns)

    //Calculate final sum in sum column
    val all_counts=countsWithZeros.select( Array[Column]($"value") ++ countColumns.map(fieldName => col(fieldName)): _*)
      .withColumn("sum",countColumns.map(fieldName => col(fieldName)).reduce(_+_))

    //Display the cooked result
    all_counts.sort($"sum".desc)
  }

  def formatDF(finalDF: DataFrame): DataFrame ={
    val spark: SparkSession = finalDF.sparkSession
    import spark.implicits._
    val countColumns = finalDF.columns.filter(colName => !Array("value", "sum").contains(colName))
    val formattedCounts = finalDF.withColumn("print_sum_string", concat_ws(" + ", countColumns.map(fieldName => col(fieldName)) : _*))
      .drop(countColumns : _*)
    val formattedValSum = formattedCounts.withColumn("print_val_sum", concat_ws(" ", $"value", $"sum")).drop("value", "sum")
    val formattedDF = formattedValSum.withColumn("print_column", concat_ws(" = ", $"print_val_sum", $"print_sum_string")).drop("print_sum_string", "print_val_sum")
    formattedDF
  }
}
