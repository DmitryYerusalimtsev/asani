package com.dyeru.asani.arrow

import org.apache.arrow.vector.{IntVector, VarCharVector}

case class Frame(image: Array[Byte], metadata: String, timestamp: Long)

@main
def main(): Unit = {
  //  val schema = CaseClassToArrowSchema.toArrowSchema[Frame]
  //  println(schema)
  case class User(id: Int, name: String, nick: Option[String], bytes: Array[Byte])

//  val s = new ArrowSchema {}.schemaOf[User]
//  println(s)

  case class Person(name: String, age: Int, active: Boolean)

  val person = Person("Alice", 30, true)
//  val d = ToMap[Person].toMap(person)
  val people = Seq(person, person).toArrowVector
//  val d = ToVector().toVector(people)
  println(people.contentToTSVString())

  // Assertions to check if the Arrow Vector contains expected values
  val nameVector = people.getVector("name").asInstanceOf[VarCharVector]
  assert(new String(nameVector.get(0)) == "Alice")
  assert(new String(nameVector.get(1)) == "Bob")

  val ageVector = people.getVector("age").asInstanceOf[IntVector]
  assert(ageVector.get(0) == 30)
  assert(ageVector.get(1) == 25)
}