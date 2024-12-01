package com.dyeru.asani.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import java.time.Instant
import scala.jdk.CollectionConverters.*

def createVectorSchemaRoot(fields: Seq[Field], data: Seq[Seq[Any]]): VectorSchemaRoot = {
  val allocator = new RootAllocator(Long.MaxValue)
  val vectors = fields.map { field =>
    val vector = field.getFieldType.getType match {
      case _: ArrowType.Int =>
        val intVector = new IntVector(field.getName, allocator)
        intVector.allocateNew()
        intVector
      case _: ArrowType.FloatingPoint =>
        val floatVector = new Float8Vector(field.getName, allocator)
        floatVector.allocateNew()
        floatVector
      case _: ArrowType.Utf8 =>
        val textVector = new VarCharVector(field.getName, allocator)
        textVector.allocateNew()
        textVector
      case _: ArrowType.Bool =>
        val boolVector = new BitVector(field.getName, allocator)
        boolVector.allocateNew()
        boolVector
      case _: ArrowType.Timestamp =>
        val timestampVector = new TimeStampMilliVector(field.getName, allocator)
        timestampVector.allocateNew()
        timestampVector
      case _: ArrowType.Binary =>
        val binaryVector = new VarBinaryVector(field.getName, allocator)
        binaryVector.allocateNew()
        binaryVector
      case _ => throw new IllegalArgumentException("Unsupported type")
    }
    vector
  }

  val root = new VectorSchemaRoot(fields.asJava, vectors.asJava, data.headOption.map(_.size).getOrElse(0))

  // Fill the vectors with data
  for ((row, i) <- data.zipWithIndex; (value, j) <- row.zipWithIndex) {
    val vector = vectors(j)
    vector match {
      case v: IntVector => v.setSafe(i, value.asInstanceOf[Int])
      case v: Float8Vector => v.setSafe(i, value.asInstanceOf[Double])
      case v: VarCharVector =>
        val bytes = value.asInstanceOf[String].getBytes
        v.setSafe(i, bytes, 0, bytes.length)
      case v: BitVector => v.setSafe(i, if (value.asInstanceOf[Boolean]) 1 else 0)
      case v: TimeStampMilliVector => v.setSafe(i, value.asInstanceOf[Instant].toEpochMilli)
      case v: VarBinaryVector => v.setSafe(i, value.asInstanceOf[Array[Byte]])
    }
  }

  root.setRowCount(data.length)

  root
}