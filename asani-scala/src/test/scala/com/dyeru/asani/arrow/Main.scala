package com.dyeru.asani.arrow

import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import com.dyeru.asani.arrow.flight.*
import org.apache.arrow.memory.RootAllocator

case class Frame(image: Array[Byte], metadata: String, timestamp: Long)

@main
def main(): Unit = {
  //  val schema = CaseClassToArrowSchema.toArrowSchema[Frame]
  //  println(schema)
  case class User(id: Int, name: String, nick: Option[String], bytes: Array[Byte])

//  val s = new ArrowSchema {}.schemaOf[User]
//  println(s)

  case class Person(name: String, age: Int, active: Boolean)

  val allocator = new RootAllocator(Long.MaxValue)
  val root = VectorSchemaRoot.create(ArrowSchema.derived[Person].schema, allocator)
  
  val person = Person("Alice", 30, true)
  val person2 = Person("Ben", 42, false)

  val people = Seq(person, person2).toArrowVector(root)
  println(people.contentToTSVString())

  val recoveredPeople: List[Person] = people.toProducts

  println(recoveredPeople)

  people.close()

  val processor = new Processor[Person, Person] {
    def process(in: List[Person]): List[Person] = in
  }
  Server(processor).start()
}
