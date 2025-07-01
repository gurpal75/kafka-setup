package fr.enedis

import fr.enedis.protobuf.PersonProtos.Person
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

object ProtobufKafkaConsumer {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val topic = "demo-topic"
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-consumer-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // pour consommer depuis le début

    val consumer = new KafkaConsumer[String, Array[Byte]](props)
    consumer.subscribe(Collections.singletonList(topic))
    log.info("🚀 One-shot consumer started.")

    val maxIdlePolls = 3 // Nombre max de polls vides avant arrêt
    var idlePolls = 0
    while (idlePolls < maxIdlePolls) {
      val records = consumer.poll(Duration.ofSeconds(10))
      if (records.isEmpty) {
        idlePolls += 1
        log.info(s"⏳ No new message... (${idlePolls}/${maxIdlePolls})")
      } else {
        idlePolls = 0 // reset si des messages arrivent
        for (record <- records.asScala) {
          val person = Person.parseFrom(record.value())
          log.info(s"📨 Received: name=${person.getName}, age=${person.getAge}, email=${person.getEmail}")
        }
      }
    }
    consumer.close()
    log.info("✅ No more messages. Consumer stopped.")

  }
}
