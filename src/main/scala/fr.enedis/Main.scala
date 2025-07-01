package fr.enedis
import utils.ProtobufHelper

object Main extends App {
  val listName : List[String] = List("shreesha", "zunzun", "kevin", "rahma", "manel", "kodjo")
  var age = 26
  for (name <- listName) {
    val email = s"${name}@example.com"
    val person = ProtobufHelper.createPerson(name, age, email)
    ProtobufHelper.writeToFile(person, s"${name}.pb")
    age += 1
  }
}
