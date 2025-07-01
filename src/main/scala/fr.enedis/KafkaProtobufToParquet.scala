package fr.enedis


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import fr.enedis.protobuf.PersonProtos.Person

object KafkaProtobufToParquet extends App{
  val spark = SparkSession.builder()
    .appName("KafkaProtobufToParquet")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Lecture depuis Kafka
  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "demo-topic")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  // UDF pour décoder les données Protobuf
  val decodeProtobuf = udf((bytes: Array[Byte]) => {
    val person = Person.parseFrom(bytes)
    (person.getName, person.getAge, person.getEmail)
  })

  val decodedDF = kafkaDF
    .select(decodeProtobuf(col("value")).as("person"))
    .selectExpr("person._1 as name", "person._2 as age", "person._3 as email")

  // Écriture en Parquet
  decodedDF.writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", "C:/tmp/spark-checkpoint")
    .option("path", "C:/tmp/person-parquet-output")
    .start()
    .awaitTermination()
}
