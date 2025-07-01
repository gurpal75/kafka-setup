package fr.enedis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaProtobufStreamer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaProtobufStreamer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    println("✅ SparkSession started.")

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "demo-topic")
      .option("startingOffsets", "earliest")
      .load()

    val rawValues = kafkaDf.selectExpr("value")



    println("🔁 Kafka stream source initialisée.")
    spark.streams.awaitAnyTermination()
  }
}
