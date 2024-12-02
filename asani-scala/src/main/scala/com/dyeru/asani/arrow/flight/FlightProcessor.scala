package com.dyeru.asani.arrow.flight

import com.dyeru.asani.Processor
import com.dyeru.asani.arrow.*
import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import scala.deriving.Mirror
import scala.deriving.Mirror.ProductOf
import scala.util.Using

trait FlightProcessor[
  In: ToProduct : Mirror.ProductOf,
  Out: ToMap : ToVector : Mirror.ProductOf : ArrowSchema
] extends Processor[In, Out] {

  def processRequest(context: CallContext,
                     reader: FlightStream,
                     writer: ServerStreamListener,
                     allocator: RootAllocator): Unit = {
    val arrowSchema = implicitly[ArrowSchema[Out]]

    Using(VectorSchemaRoot.create(arrowSchema.schema, allocator)) { root =>
      while (reader.next()) {
        if (reader.hasRoot) {
          val requestData: List[In] = reader.getRoot.toProducts
          writer.start(root)
          writer.setUseZeroCopy(true)
          process(requestData).toArrowVector(root)
          writer.putNext(reader.getLatestMetadata)
        }
      }
      writer.completed()
    }
  }
}
