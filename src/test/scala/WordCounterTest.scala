import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec}
class WordCounterTest extends FlatSpec with BeforeAndAfter {
  private var spark: SparkSession = _
  private val master = "local[2]"
  private val appName = "WordCounterTest"
  before {
    val conf = new SparkConf().setMaster(master)
      .setAppName(appName)
    spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }
  after {
    if (spark != null) {
      spark.stop()
    }
  }
  "WordCounter" should " calculate word frequency per file and rolled up word frequency" in {
    val inputPath = getClass.getResource("/test_events").getPath
    val inputDataFrame = spark.read.text(inputPath)
    val calculatedDF = WordCounter.transform(spark, inputDataFrame)
    val expectedOutput = getClass.getResource("/expected_output/result").getPath
    val expectedDF = spark.read.option("header", "true").csv(expectedOutput)
    val formattedDF = WordCounter.formatDF(calculatedDF)
    calculatedDF.show()
    expectedDF.show()
    assert (calculatedDF.except(expectedDF).count()==0 && expectedDF.except(calculatedDF).count() ==0)
  }
  "WordCounter" should " prepare dataframe in proper format" in {
    val expectedOutput = getClass.getResource("/expected_output/result").getPath
    val expectedDF = spark.read.option("header", "true").csv(expectedOutput)
    val formattedDF = WordCounter.formatDF(expectedDF)
    val expectedFormattedOutput = getClass.getResource("/expected_output/result_formatted").getPath
    val expectedFormattedOutputDF = spark.read.text(expectedFormattedOutput)
    assert (formattedDF.except(expectedFormattedOutputDF).count()==0 && expectedFormattedOutputDF.except(formattedDF).count() ==0)
  }
}