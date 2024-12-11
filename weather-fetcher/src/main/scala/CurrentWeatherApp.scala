import org.apache.spark
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import requests.Response

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object CurrentWeatherApp {
  def main(args: Array[String]): Unit ={

    val spark: SparkSession = SparkSession.builder()
      .appName("Fetcher of December Big Data Challenge")
      .master("local")
      .getOrCreate()

    val city: String = args(0)
    val currentEndpoint: String = "https://api.open-meteo.com/v1/forecast"
    val currentDateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val timestampString = currentDateTime.format(formatter)
    val coords = Cities.citiesCoords(city)
    val currentQueryParams = Map(
      "latitude" -> coords._1,
      "longitude" -> coords._2,
      "timezone" -> "auto",
      "current" -> "temperature_2m,relative_humidity_2m,surface_pressure,weather_code"
    )
    val r: Response = requests.get(currentEndpoint, params = currentQueryParams)
    val responseDF: Dataset[Row] = changeDataToJson(spark, r)
    val formatterDetailed = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
    val timestampStringDetailed = currentDateTime.format(formatterDetailed)
    responseDF.write.mode(SaveMode.Overwrite).format("json").save(s"current/$city/$timestampStringDetailed")
    println("yo bros!")
  }

  def changeDataToJson(spark: SparkSession, r: Response): Dataset[Row]={
    import spark.implicits._
    val jsonDF = spark.read.json(Seq(r.text).toDS)
    jsonDF
  }

}
