package com.dyeru.asani.arrow

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.InvalidArrowFileException
import org.apache.arrow.vector.types.pojo.Schema

import scala.deriving.Mirror
import scala.util.Using
import scala.jdk.CollectionConverters.*
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.annotation.tailrec

extension [F[_] <: Seq[_], T: Mirror.ProductOf](values: F[T])
  inline def toArrowVector: VectorSchemaRoot = ToVector[T]().toVector(values.asInstanceOf[Seq[T]])

trait ToVector[T] {
  def toVector(values: Seq[T]): VectorSchemaRoot
}

object ToVector extends ArrowSchema {
  inline def apply[T: Mirror.ProductOf : ToMap]: ToVector[T] =
    val schema: Schema = schemaOf[T]

    (values: Seq[T]) => {
      val records = values.map(implicitly[ToMap[T]].toMap)

      Using(new RootAllocator()) { allocator =>
        Using(VectorSchemaRoot.create(schema, allocator)) { root =>
          root
            .getFieldVectors
            .asScala
            .foreach(vector => Using(vector) { vec =>
              vec match
                case v: FixedWidthVector => v.allocateNew(records.length)
                case v: VariableWidthFieldVector => v.allocateNew(records.length)

              records
                .map(_(vec.getName))
                .zipWithIndex
                .foreach((value, index) => setField(vec, index, value))

              vec.setValueCount(records.length)
            })

          root.setRowCount(values.length)
          root
        }
      }.flatten.getOrElse(throw new InvalidArrowFileException(""))
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
        writer.startList()
        v.zipWithIndex.foreach { (elem, i) =>
          writer.setPosition(i)
          writer.writeVarChar(elem.toString)
        }
        writer.setValueCount(v.length)
        writer.endList()

      case v: Option[_] => v match
        case Some(iv) => setField(vector, index, iv)
        case None => throw new Exception("temp") // vector.getValidityBuffer.asInstanceOf.setB(1, false)
    }
}
