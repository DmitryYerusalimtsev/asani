package com.dyeru.asani.arrow

import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionListWriter

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*

extension [F[_] <: Seq[_], T: Mirror.ProductOf](values: F[T])(using instance: ToVector[T])
  def toArrowVector(root: VectorSchemaRoot): VectorSchemaRoot =
    instance.toVector(values.asInstanceOf[Seq[T]], root)

trait ToVector[T] {
  def toVector(values: Seq[T], root: VectorSchemaRoot): VectorSchemaRoot
}

object ToVector {
  def apply[T](using toVector: ToVector[T]): ToVector[T] = toVector

  private inline def derived[T](using p: Mirror.ProductOf[T]): ToVector[T] = {
    (values: Seq[T], root: VectorSchemaRoot) => {

      val records = values.map(implicitly[ToMap[T]].toMap)

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
      case v: Int => vector.asInstanceOf[IntVector].setSafe(index, v)
      case v: Long => vector.asInstanceOf[BigIntVector].setSafe(index, v)
      case v: String => vector.asInstanceOf[VarCharVector].setSafe(index, v.getBytes(StandardCharsets.UTF_8))
      case v: Double => vector.asInstanceOf[Float8Vector].setSafe(index, v)
      case v: Float => vector.asInstanceOf[Float4Vector].setSafe(index, v)
      case v: Boolean => vector.asInstanceOf[BitVector].setSafe(index, if v then 1 else 0)
      case v: Array[Byte] => vector.asInstanceOf[LargeVarBinaryVector].setSafe(index, v)
      case v: Instant => vector.asInstanceOf[TimeStampMilliVector].setSafe(index, v.toEpochMilli)

      case v: Seq[_] =>
        val writer = vector.asInstanceOf[ListVector].getWriter
        writer.setPosition(index)
        writer.startList()
        v.foreach(elem => writeListRecursive(writer, elem))
        writer.endList()
        writer.setValueCount(v.length)

      case v: Option[_] => v match
        case Some(iv) => setField(vector, index, iv)
        case None => vector.setNull(index)
    }
  
  @tailrec
  private def writeListRecursive(writer: UnionListWriter, value: Any): Unit = {
    value match {
      case v: Int => writer.writeInt(v)
      case v: Long => writer.writeBigInt(v)
      case v: Float => writer.writeFloat4(v)
      case v: Double => writer.writeFloat8(v)
      case v: Boolean => writer.writeBit(if v then 1 else 0)
      case v: String => writer.writeVarChar(v)
      case v: Instant => writer.writeBigInt(v.toEpochMilli)
      case v: Option[_] => v match
        case Some(inner) => writeListRecursive(writer, inner)
        case None => writer.writeNull()
      case other => throw new IllegalArgumentException(s"Unsupported type in list: ${other.getClass}")
    }
  }

  // Enable derivation for case classes
  inline given derivedToVector[T](using m: Mirror.ProductOf[T]): ToVector[T] = derived
}
