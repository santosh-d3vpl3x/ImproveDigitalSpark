import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession

/**
  * Spark app to compute sum of words from multiple files and calculate rollups.
  * Not going by the way of rollup/cube in spark as the output of those commands are not in desired format.
  * This app can be extended to n number of files.
  */
object WordCounter {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("SparkFlattenedVEONEventETL").getOrCreate()
    val get_file_name=spark.udf.register("get_file_name", (fullPath: String) => fullPath.split("/").last)

    import spark.implicits._

    //Calculate file name pivoted count of each word
    val fileWiseCounts = spark.read.text("/Users/santosh/IdeaProjects/ImproveDigitalSpark/src/main/resources/file1","/Users/santosh/IdeaProjects/ImproveDigitalSpark/src/main/resources/file2")
      .select(get_file_name(input_file_name).as("file_name"), split($"value","\\W").as("values"))
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
    all_counts.sort($"sum".desc).show()
  }
}
