package com.dyeru.asani.arrow.flight

import com.dyeru.asani.arrow.*
import com.dyeru.asani.arrow.flight.AsaniProducer.Command
import com.dyeru.asani.arrow.flight.AsaniProducer.Command.Put
import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.memory.RootAllocator

import java.nio.charset.StandardCharsets
import scala.deriving.Mirror
import scala.deriving.Mirror.ProductOf

class Server[
  In: ToProduct : Mirror.ProductOf,
  Out: ToMap : ToVector : Mirror.ProductOf]
(processor: Processor[In, Out]) {
  
  def start(host: String = "localhost", port: Int = 47470): Unit = {
    val location = Location.forGrpcInsecure(host, port)

    val allocator = new RootAllocator(Long.MaxValue)

    val server = FlightServer
      .builder(allocator, location, new AsaniProducer(processor))
      .build()

    server.start()
    server.awaitTermination()
  }
}

class AsaniProducer[
  In: ToProduct : Mirror.ProductOf,
  Out: ToVector : ToMap : Mirror.ProductOf]
(processor: Processor[In, Out]) extends NoOpFlightProducer {
  override def doExchange(context: CallContext,
                          reader: FlightStream,
                          writer: ServerStreamListener): Unit = {
    val command = Command.fromArray(reader.getDescriptor.getCommand)

    command match
      case Put => doPut(context, reader, writer, processor)
  }

  private def doPut(context: CallContext,
                    reader: FlightStream,
                    writer: ServerStreamListener,
                    processor: Processor[In, Out]): Unit = {
    while (reader.next()) {
      if (reader.hasRoot) {
        val requestData: List[In] = reader.getRoot.toProducts
        val responseDataRoot = processor.process(requestData).toArrowVector
        writer.start(responseDataRoot)
        writer.setUseZeroCopy(true)
        writer.putNext()
        writer.putMetadata(reader.getLatestMetadata)
        writer.completed()
      }

      // TODO: CLOSE ROOT
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