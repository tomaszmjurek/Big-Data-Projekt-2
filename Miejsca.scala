package com.example.bigdata
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{row_number, col}
import org.apache.spark.sql.expressions.Window

object Miejsca {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Miejsca")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val regionyDS = spark.read.csv(args(0) + "/regions.csv")
    val wladzeDS = spark.read.csv(args(0) + "/authorities.csv")
    val drogiDS = spark.read.csv(args(0) + "/mainData.csv")
    val razemDS = regionyDS.join(wladzeDS, wladzeDS("region_ons_code") === regionyDS("region_ons_code")).
      select(
        regionyDS("region_ons_code"),
        wladzeDS("local_authority_name"),
        regionyDS("region_name"),
        wladzeDS("local_authority_ons_code")
      )
    val polaczone = razemDS.join(drogiDS, drogiDS("local_authority_ons_code") === razemDS("local_authority_ons_code")).
      select(
        razemDS("region_ons_code"),
        razemDS("local_authority_name"),
        razemDS("region_name"),
        drogiDS("road_name"),
        drogiDS("road_category"),
        drogiDS("road_type")
      )
    val window = Window.orderBy(col("region_ons_code"))
    polaczone
      .distinct()
      .withColumn("id", row_number.over(window))
      .write
      .insertInto("miejsca")
  }
}