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

  people.close()
}