package fr.enedis

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}
import fr.enedis.protobuf.PersonProtos.Person

import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.Properties

object SimpleKafkaProducer {
  val log = LoggerFactory.getLogger(this.getClass)
  LoggerFactory.getLogger(this.getClass).atInfo().log()
  def main(args: Array[String]): Unit = {
    val topic = "demo-topic"
    val folderPath = "src/main/resources/data"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)

    val pbFiles : Array[String] = Files.list(Paths.get(folderPath)).toArray().map(_.toString)
    val producer = new KafkaProducer[String, Array[Byte]](props)

    pbFiles.zipWithIndex.foreach{
      case(pth, indx) =>
        val key = s"msg-${indx}"
        val person = Person.parseFrom(new FileInputStream(pth))
        val messageBytes = person.toByteArray
        val record = new ProducerRecord[String, Array[Byte]](topic, key, messageBytes)

        producer.send(record, new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            if (e == null)
              log.info(s"[INSERTED] record to topic: ${recordMetadata.topic()}")
            else
              log.error(s"[INSERT FAILED] ${e}")
          }
        })
        Thread.sleep(10000)
    }
    producer.close()
  }
}
