import org.apache.spark.sql.SparkSession

object TestApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Fetcher of December Big Data Challenge")
      .master("local")
      .getOrCreate()

    spark.read.json("current/Lodz/*").show()
  }
}
