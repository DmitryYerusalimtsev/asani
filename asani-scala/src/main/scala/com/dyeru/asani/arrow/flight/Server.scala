package com.dyeru.asani.arrow.flight

import com.dyeru.asani.arrow.*
import com.dyeru.asani.arrow.flight.AsaniProducer.Command
import com.dyeru.asani.arrow.flight.AsaniProducer.Command.Put
import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets
import scala.deriving.Mirror
import scala.deriving.Mirror.ProductOf
import scala.util.Using

class Server[
  In: ToProduct : Mirror.ProductOf,
  Out: ToMap : ToVector : Mirror.ProductOf : ArrowSchema
]
(processor: Processor[In, Out]) {

  def start(host: String = "localhost", port: Int = 47470): Unit = {
    val location = Location.forGrpcInsecure(host, port)

    val allocator = new RootAllocator(Long.MaxValue)

    val server = FlightServer
      .builder(allocator, location, new AsaniProducer(processor, allocator))
      .build()

    server.start()
    server.awaitTermination()
  }
}

class AsaniProducer[
  In: Mirror.ProductOf : ToProduct,
  Out: Mirror.ProductOf : ToVector : ToMap : ArrowSchema
]
(
  processor: Processor[In, Out],
  allocator: RootAllocator
) extends NoOpFlightProducer {

  override def doExchange(context: CallContext,
                          reader: FlightStream,
                          writer: ServerStreamListener): Unit = {
    val command = Command.fromArray(reader.getDescriptor.getCommand)

    command match
      case Put => processRequest(context, reader, writer, processor)
  }

  private def processRequest(context: CallContext,
                             reader: FlightStream,
                             writer: ServerStreamListener,
                             processor: Processor[In, Out]): Unit = {
    val arrowSchema = implicitly[ArrowSchema[Out]]

    Using(VectorSchemaRoot.create(arrowSchema.schema, allocator)) { root =>
      while (reader.next()) {
        if (reader.hasRoot) {
          val requestData: List[In] = reader.getRoot.toProducts
          writer.start(root)
          writer.setUseZeroCopy(true)
          processor.process(requestData).toArrowVector(root)
          writer.putNext(reader.getLatestMetadata)
        }
      }
      writer.completed()
    }
  }
}


object AsaniProducer {

  enum Command:
    case Put

    def toArray: Array[Byte] = this.toString.getBytes(StandardCharsets.UTF_8)

  object Command:
    def fromArray(array: Array[Byte]): Command = {
      val command = new String(array, StandardCharsets.UTF_8)
      Command.valueOf(command)
    }
}