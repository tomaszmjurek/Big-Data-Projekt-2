package com.example.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, col, count, hour, min, monotonically_increasing_id, round, row_number, split, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.IntegerType

object Fakty {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Fakty")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");

    import spark.implicits._
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
	
	
	------------------------------------------
	
	
val mainScotland = spark.read.format("org.apache.spark.csv")
	.option("header", true)
	.option("inferSchema", true)
	.csv(s"/user/$username/labs/spark/externaldata/mainDataScotland.csv")

val mainSouthEngland = spark.read.format("org.apache.spark.csv")
	.option("header", true)
	.option("inferSchema", true)
	.csv(s"/user/$username/labs/spark/externaldata/mainDataSouthEngland.csv")

val mainNorthEngland = spark.read.format("org.apache.spark.csv")
	.option("header", true)
	.option("inferSchema", true)
	.csv(s"/user/$username/labs/spark/externaldata/mainDataNorthEngland.csv")


val allTraffic = mainScotland
  .union(mainSouthEngland)
  .union(mainNorthEngland)
  .dropDuplicates(Array("ID"));
  
------------------------------------


//trzeba zdefiniować tak, aby posiadał wszystkie pola pokrywające się z następujacymi danymi (opisane w komentarzach)
val allTrafficWithTime = allTraffic.


------------------------------------

val locationDF = spark.sql("SELECT * FROM miejsca")
val timeDF = spark.sql("SELECT * FROM czas")
val typesDF = spark.sql("SELECT * FROM typy_pojazdow")
val weatherDF = spark.sql("SELECT * FROM pogoda")


//allTrafficWithTime musi mieć miesiac i dzien albo date jak w tabeli 'czas', można przyciąć count_date
val trafficTimes = allTrafficWithTime.join(timeDF,
	timeDF("godzina")===allTrafficWithTime("hour")&&
	timeDF("data")===allTrafficWithTime("date")
).select(allTrafficWithTime("ID").as("id"), $"id_czasu")


//allTrafficWithTime musi mieć typ pojazdu i kategorie zgrupowane jak w tabeli 'typy_pojazdow'
val trafficTypes = allTrafficWithTime.join(typesDF,
	typesDF("typ")===allTrafficWithTime("type")&&
	typesDF("kategoria")===allTrafficWithTime("category")
).select(allTrafficWithTime("ID").as("id"), $"id_pojazdu")
	

//allTrafficWithTime musi mieć też opis pogody jak w tabeli 'pogoda'
val trafficWeather = allTrafficWithTime.join(weatherDF,
	weatherDF("opis_pogody")===allTrafficWithTime("weather_description")
).select(allTrafficWithTime("ID").as("id"), $"id_pogody")


val trafficLocation = allTrafficWithTime.join(locationDF,
	weatherDF("kod_ons_obszaru")===allTrafficWithTime("local_authoirty_ons_code") &&
	weatherDF("nazwa_regionu")===allTrafficWithTime("local_authoirty_ons_code") &&
	weatherDF("nazwa_drogi")===allTrafficWithTime("local_authoirty_ons_code") &&
	weatherDF("kategoria_drogi")===allTrafficWithTime("local_authoirty_ons_code") &&
	weatherDF("typ_drogi")===allTrafficWithTime("local_authoirty_ons_code")
).select(allTrafficWithTime("ID").as("id"), $"id_miejsca")


//dopisać liczenie pojazdów
val finalTable = trafficTimes
      .join(trafficLocation, trafficLocation("id") === trafficTimes("id"))
      .join(trafficTypes, trafficTypes("id") === trafficTimes("id"))
      .join(trafficWeather, trafficWeather("id") === trafficTimes("id"))
      .select($"id_czasu", $"id_pojazdu", $"id_miejsca", $"id_pogody", count(...).as("liczba_pojazdow"))
	  
//finalDataDF.printSchema()
//finalDataDF.show()
finalTable.write.insertInto("fakty")
//finalTable.printSchema()
println("Załadowano tabele faktow")

}
}
