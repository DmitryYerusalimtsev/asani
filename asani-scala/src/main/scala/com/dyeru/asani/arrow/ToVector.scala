package com.dyeru.asani.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.Schema

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*

extension[F[_] <: Seq[_], T: Mirror.ProductOf](values: F[T]) (using instance: ToVector[T] )
  def toArrowVector: VectorSchemaRoot = instance.toVector(values.asInstanceOf[Seq[T]])

trait ToVector[T] {
  def toVector(values: Seq[T]): VectorSchemaRoot
}

object ToVector {
  def apply[T](using toVector: ToVector[T]): ToVector[T] = toVector

  private inline def derived[T](using p: Mirror.ProductOf[T]): ToVector[T] = {
    (values: Seq[T]) => {
      val schema: Schema = ArrowSchema.schemaOf[T]

      val records = values.map(implicitly[ToMap[T]].toMap)

      val allocator = new RootAllocator()
      val root = VectorSchemaRoot.create(schema, allocator)
      root.allocateNew()

      root
        .getFieldVectors
        .asScala
        .foreach(vector =>
          records
            .map(_(vector.getName))
            .zipWithIndex
            .foreach((value, index) => setField(vector, index, value))

          vector.setValueCount(records.length)
        )

      root.setRowCount(values.length)
      root
    }
  }

  @tailrec
  private def setField(vector: FieldVector, index: Int, value: Any): Unit =
    value match {
      case v: Int => vector.asInstanceOf[IntVector].set(index, v)
      case v: Long => vector.asInstanceOf[BigIntVector].set(index, v)
      case v: String => vector.asInstanceOf[VarCharVector].set(index, v.getBytes(StandardCharsets.UTF_8))
      case v: Double => vector.asInstanceOf[Float8Vector].set(index, v)
      case v: Float => vector.asInstanceOf[Float4Vector].set(index, v)
      case v: Boolean => vector.asInstanceOf[BitVector].set(index, if v then 1 else 0)
      case v: Array[Byte] => vector.asInstanceOf[VarBinaryVector].set(index, v)
      case v: Instant => vector.asInstanceOf[TimeStampMilliVector].set(index, v.toEpochMilli)

      case v: Seq[_] =>
        val writer = vector.asInstanceOf[ListVector].getWriter
        writer.setPosition(index)
        writer.startList()
        v.zipWithIndex.foreach((elem, i) => writer.writeVarChar(elem.toString))
        writer.endList()
        writer.setValueCount(v.length)

      case v: Option[_] => v match
        case Some(iv) => setField(vector, index, iv)
        case None => vector.setNull(index)
    }

  // Enable derivation for case classes
  inline given derivedToVector[T](using m: Mirror.ProductOf[T]): ToVector[T] = derived
}
