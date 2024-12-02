package com.dyeru.asani.arrow.flight

import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.memory.RootAllocator

import java.nio.charset.StandardCharsets

class Server(processors: List[FlightProcessor[_, _]]) {

  def start(host: String = "localhost", port: Int = 47470): Unit = {
    val location = Location.forGrpcInsecure(host, port)

    val allocator = new RootAllocator(Long.MaxValue)

    val server = FlightServer
      .builder(allocator, location, new AsaniProducer(processors, allocator))
      .build()

    server.start()
    server.awaitTermination()
  }
}

class AsaniProducer(
                     processors: List[FlightProcessor[_, _]],
                     allocator: RootAllocator
                   ) extends NoOpFlightProducer {

  override def doExchange(context: CallContext,
                          reader: FlightStream,
                          writer: ServerStreamListener): Unit = {

    val command = new String(reader.getDescriptor.getCommand, StandardCharsets.UTF_8)

    processors.find(_.command == command) match
      case Some(p) => p.processRequest(context, reader, writer, allocator)
      case None => throw new IllegalArgumentException(s"No registered processor for command: $command")
  }
}
