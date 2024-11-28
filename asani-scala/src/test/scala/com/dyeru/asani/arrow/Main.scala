package com.dyeru.asani.arrow

case class Frame(image: Array[Byte], metadata: String, timestamp: Long)

@main
def main(): Unit = {
  //  val schema = CaseClassToArrowSchema.toArrowSchema[Frame]
  //  println(schema)
  case class User(id: Int, name: String, nick: Option[String], bytes: Array[Byte])

  val s = new ArrowSchema {}.of[User]

  println(s)
}