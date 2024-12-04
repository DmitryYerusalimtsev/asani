package com.dyeru.asani.arrow.flight

import com.dyeru.asani.AsaniException
import com.dyeru.asani.arrow.*
import org.apache.arrow.flight.{FlightClient, FlightDescriptor, FlightStream, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import scala.deriving.Mirror
import scala.util.{Failure, Success, Using}

class Client(host: String, port: Int = 8815) extends AutoCloseable {

  private val location = Location.forGrpcInsecure(host, port)
  private val allocator = new RootAllocator(Long.MaxValue)

  private val client = FlightClient.builder(allocator, location).build()

  def call[
    Req: ToMap : ToVector : Mirror.ProductOf : ArrowSchema,
    Resp: ToProduct : Mirror.ProductOf
  ]
  (command: String, data: Seq[Req])(using ExecutionContext): Future[List[Resp]] = {

    val schema = implicitly[ArrowSchema[Req]].schema

    Future {
      Using(VectorSchemaRoot.create(schema, allocator)) { root =>
        val descriptor = FlightDescriptor.command(command.getBytes(StandardCharsets.UTF_8))
        val clientStream: FlightClient.ExchangeReaderWriter = client.doExchange(descriptor)
        val writer: FlightClient.ClientStreamListener = clientStream.getWriter

        writer.start(root)
        data.toArrowVector(root)
        writer.putNext()
        writer.completed()

        val reader: FlightStream = clientStream.getReader
        reader.next()

        val responseRoot: VectorSchemaRoot = reader.getRoot
        val result: List[Resp] = responseRoot.toProducts
        responseRoot.close()
        result
      }
      match
        case Success(value) => value
        case Failure(e) => throw new AsaniException("Failed to communicate to specified Asani server.", e)
    }
  }

  override def close(): Unit = {
    client.close()
  }
}
