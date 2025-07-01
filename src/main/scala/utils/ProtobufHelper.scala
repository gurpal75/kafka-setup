package utils

import fr.enedis.protobuf.PersonProtos.Person

import java.io.FileOutputStream

object ProtobufHelper {
  def createPerson(name: String, age: Int, email: String): Person = {
    Person.newBuilder()
      .setName(name)
      .setAge(age)
      .setEmail(email)
      .build()
  }

  def writeToFile(person: Person, path: String): Unit = {
    val output = new FileOutputStream("src/main/resources/data/" + path)
    try {
      person.writeTo(output)
    } finally {
      output.close()
    }
  }

}
