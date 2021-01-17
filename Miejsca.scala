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

    val regionyDS1 = spark.read.csv(args(0) + "/regionsScotland.csv")
    val regionyDS2 = spark.read.csv(args(0) + "/regionsSouthEngland.csv")
    val regionyDS3 = spark.read.csv(args(0) + "/regionsNorthEngland.csv")
    val regionyDS = regionyDS1.
      union(regionyDS2).
      union(regionyDS3)
    val wladzeDS1 = spark.read.csv(args(0) + "/authoritiesScotland.csv")
    val wladzeDS2 = spark.read.csv(args(0) + "/authoritiesNorthEngland.csv")
    val wladzeDS3 = spark.read.csv(args(0) + "/authoritiesSouthEngland.csv")
    val wladzeDS = wladzeDS1.
      union(wladzeDS2).union(wladzeDS3)
    val drogiDS1 = spark.read.csv(args(0) + "/mainDataScotland.csv")
    val drogiDS2 = spark.read.csv(args(0) + "/mainDataSouthEngland.csv")
    val drogiDS3 = spark.read.csv(args(0) + "/mainDataNorthEngland.csv")
    val drogiDS = drogiDS1.union(drogiDS2).union(drogiDS3)
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