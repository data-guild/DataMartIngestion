import java.sql.DriverManager
import java.sql.Connection

import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object DataMartIngestion {
  val stationSchema = new StructType()
    .add("stations", ArrayType(
      new StructType()
      .add("empty_slots", ShortType)
      .add("free_bikes",ShortType)
      .add("id",StringType)
      .add("latitude", FloatType)
      .add("longitude", FloatType)
      .add("name", StringType)
      .add("timestamp", StringType)
      .add("extra",
        new StructType()
          .add("last_updated", IntegerType)
          .add("renting", ShortType)
          .add("returning", ShortType)
          .add("uid", StringType)
          .add("address", StringType)
      )))

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("DataMartIngestion")
      .master("local[*]")
      .getOrCreate()

    val fileName = "src/main/resources/stations_nyc"
    prepareParquetFile(spark, fileName)

    val stationDF = spark.read.parquet(fileName).select("id", "name", "longitude", "latitude", "empty_slots", "free_bikes")

    //    // create the statement, and run the select query
//    val connection = connectToDb()
//    val statement = connection.createStatement()
//    val resultSet = statement.executeQuery("SELECT * FROM stations")
//
//    while ( resultSet.next() ) {
//      println("result stationId: ", resultSet.getString(1))
//      println("result name: ", resultSet.getString(2))
//      println("result location: ", resultSet.getString(3))
//    }
//    connection.close()
  }

  def connectToDb(): Connection = {
    val url = "jdbc:postgresql://localhost:5432/testDb"
    val username = "shelvia.hotama"
    val password = ""

    var connection : Connection = null
    try {
      Class.forName("org.postgresql.Driver")
      connection = DriverManager.getConnection(url, username, password)
      println("Connection to db is successful")
    } catch {
      case e => println("Connection unsuccessful: ", e.printStackTrace())
    }
    connection
  }

  def prepareParquetFile(spark: SparkSession, fileName: String): DataFrame = {
    val stationsDF = spark.read.schema(stationSchema).json("src/main/resources/stations-nyc.json").select(explode(col("stations")).as("station"))
      .select(col("station.id"), col("station.name"), col("station.longitude"), col("station.latitude"), col("station.empty_slots"), col("station.free_bikes"), col("station.extra.last_updated"))
    stationsDF.write.mode("overwrite").parquet(fileName)

    stationsDF
  }
}
