package com.dyeru.asani.arrow.flight

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters.*

class ClientTest extends AnyFunSuite with Matchers {

  case class Person(name: String, age: Int)

  case class PersonEnriched(name: String, age: Int, enriched: Boolean)

  // Create a simple processor that echoes input data
  private val processor = new FlightProcessor[Person, PersonEnriched] {
    def command: String = "PUT"

    def process(in: List[Person]): List[PersonEnriched] = in.map(p => PersonEnriched(p.name, p.age, true))
  }

  // Define the test server
  val server = new Server(List(processor))

  // RootAllocator for Arrow Flight
  val allocator = new RootAllocator(Long.MaxValue)

  test("Client should successfully receive correct server data") {
    // Start the server in a separate thread
    val host = "localhost"
    val port = 47470
    val serverThread = new Thread(() => server.start(host, port))
    serverThread.start()

    // Wait for the server to be ready
    Thread.sleep(1000)

    val data = List(
      Person("Alice", 30),
      Person("Bob", 25)
    )

    given ExecutionContext = ExecutionContext.global

    val client = Client(host, port)
    val response = client.call[Person, PersonEnriched]("PUT", data)
    val result = Await.result(response, 5.seconds)

    result.indices.foreach { idx =>
      result(idx).name shouldEqual data(idx).name
      result(idx).age shouldEqual data(idx).age
      result(idx).enriched shouldEqual true
    }

    serverThread.interrupt()
  }
}