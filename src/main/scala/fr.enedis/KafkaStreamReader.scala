package fr.enedis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.google.protobuf.Descriptors
import org.apache.spark.sql.protobuf.functions.from_protobuf
object KafkaStreamReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Kafka Protobuf Stream Reader")
      .master("local[*]") // change selon ton besoin
      .getOrCreate()

    import spark.implicits._

    // Lecture du stream Kafka
    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // ou kafka:29092 si tu lances dans le même réseau docker
      .option("subscribe", "demo-topic") // Remplace par le vrai topic Kafka
      .option("startingOffsets", "earliest")
      .load()

    val parsedDf = kafkaDf
      .selectExpr("CAST(value AS BINARY) as value")
      .withColumn(
        "person",
        from_protobuf(
          col("value"),
          "Person",
          "src/main/resources/schema/person.desc"
        )
      )
      .select("person.*")

    // Afficher dans la console
    val query = parsedDf.writeStream
      .format("parquet")
      .option("path", "C:/Users/gurjo/Desktop/parquetData") // <-- Remplace TonNom par ton nom d'utilisateur
      .option("checkpointLocation", "C:/Users/gurjo/Desktop/parquetData/checkpointPath")
      .outputMode("append")
      .start()

    query.awaitTermination()

  }
}
