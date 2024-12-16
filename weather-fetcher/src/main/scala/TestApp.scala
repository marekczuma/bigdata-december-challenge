import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TestApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Fetcher of December Big Data Challenge")
      .master("local")
      .getOrCreate()

    val city: String = args(0)

    val weatherDF: Dataset[Row] = spark.read.json(s"current/$city/*")

    weatherDF.show(false)

    weatherDF.printSchema()

    val finalDF = weatherDF.select("current.*", "latitude", "longitude", "timezone", "timezone_abbreviation", "utc_offset_seconds")
      .withColumn("city", lit(city))
      .withColumn("processed_timestamp", current_timestamp())

//    finalDF.show(false)
    finalDF.write.parquet(s"final/$city")
  }
}
