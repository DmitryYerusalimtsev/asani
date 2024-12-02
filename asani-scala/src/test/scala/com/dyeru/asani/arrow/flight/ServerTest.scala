package com.dyeru.asani.arrow.flight

import org.apache.arrow.flight.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BitVector, IntVector, VarCharVector, VectorSchemaRoot}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.arrow.vector.types.pojo.*

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*

class ServerTest extends AnyFunSuite with Matchers {

  case class Person(name: String, age: Int)
  case class PersonEnriched(name: String, age: Int, enriched: Boolean)

  // Create a simple processor that echoes input data
  val processor = new FlightProcessor[Person, PersonEnriched] {
    def command: String = "PUT"

    def process(in: List[Person]): List[PersonEnriched] = in.map(p => PersonEnriched(p.name, p.age, true))
  }

  // Define the test server
  val server = new Server(List(processor))

  // RootAllocator for Arrow Flight
  val allocator = new RootAllocator(Long.MaxValue)

  test("Server should start and process data correctly") {
    // Start the server in a separate thread
    val host = "localhost"
    val port = 47470
    val serverThread = new Thread(() => server.start(host, port))
    serverThread.start()

    // Wait for the server to be ready
    Thread.sleep(1000)

    // Create a client to communicate with the server
    val location = Location.forGrpcInsecure(host, port)
    val client = FlightClient.builder(allocator, location).build()

    // Define the schema for Person
    val schema = new Schema(
      List(
        new Field("name", FieldType.notNullable(new ArrowType.Utf8), null),
        new Field("age", FieldType.notNullable(new ArrowType.Int(32, true)), null)
      ).asJava
    )

    // Prepare data for the test
    val data = List(
      Person("Alice", 30),
      Person("Bob", 25)
    )

    // Convert the data to Arrow vectors
    val root = VectorSchemaRoot.create(schema, allocator)

    // Create a FlightDescriptor for the command
    val command = "PUT".getBytes(StandardCharsets.UTF_8)
    val descriptor = FlightDescriptor.command(command)

    // Send data to the server
    val clientStream: FlightClient.ExchangeReaderWriter = client.doExchange(descriptor)

    val writer: FlightClient.ClientStreamListener = clientStream.getWriter;
    writer.start(root)

    root.allocateNew()

    val nameVector = root.getVector("name").asInstanceOf[VarCharVector]
    val ageVector = root.getVector("age").asInstanceOf[IntVector]

    data.zipWithIndex.foreach { case (person, idx) =>
      nameVector.set(idx, person.name.getBytes)
      ageVector.set(idx, person.age)
    }
    nameVector.setValueCount(data.size)
    ageVector.setValueCount(data.size)

    root.setRowCount(data.size)

    writer.putNext()
    writer.completed()

    // Validate the response
    val reader: FlightStream = clientStream.getReader
    val responseRoot: VectorSchemaRoot = reader.getRoot
    reader.next()

    val responseNameVector = responseRoot.getVector("name").asInstanceOf[VarCharVector]
    val responseAgeVector = responseRoot.getVector("age").asInstanceOf[IntVector]
    val responseEnrichedVector = responseRoot.getVector("enriched").asInstanceOf[BitVector]

    responseRoot.getRowCount shouldEqual data.size

    (0 until responseRoot.getRowCount).foreach { idx =>
      val name = new String(responseNameVector.get(idx))
      val age = responseAgeVector.get(idx)
      val enriched = responseEnrichedVector.get(idx)
      name shouldEqual data(idx).name
      age shouldEqual data(idx).age
      enriched shouldEqual 1
    }

    // Close client and server
    root.close()
    responseRoot.close()
    client.close()
    serverThread.interrupt()
  }
}