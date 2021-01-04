package com.example.bigdata
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, col}

object PogodaETL {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("PogodaETL")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val weatherDS = spark.read.textFile(args(0) + "/weather.txt")
    val window = Window.orderBy(col("opis_pogody"))
    weatherDS
      .map(line => getWeatherConditionsFromLine(line))
      .distinct()
      .withColumnRenamed("value", "opis_pogody")
      .withColumn("id", row_number.over(window))
      .select(col("id"), col("opis_pogody"))
      .write
      .insertInto("pogoda")

  }

  def getWeatherConditionsFromLine(line: String): String = {
    val pattern = """^.+ the following weather conditions were reported: (.+)$""".r
    line match {
      case pattern(conditions) => conditions
      case _ => ""
    }
  }
}
