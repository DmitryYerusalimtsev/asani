package com.dyeru.asani.arrow.flight

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

case class Frame(streamName: String, image: Array[Byte])

case class Detection(stream: String, label: String, score: Double, bbox: Seq[Float])

@main
def main(): Unit = {
  given ExecutionContext = ExecutionContext.global

  val data = List(
    Frame("camera", Array(1, 2, 3, 4))
  )

  val client = Client("localhost", 8815)
  val response = client.call[Frame, Detection]("object_tracking", data)
  val result = Await.result(response, 3.seconds)
  println(result)
}