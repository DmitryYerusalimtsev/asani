package com.dyeru.asani.arrow

import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.time.Instant
import scala.compiletime.{constValueTuple, erasedValue, summonInline}
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*

trait ArrowSchema {

  inline def of[A](using p: Mirror.ProductOf[A]): Schema = {
    val labels = constValueTuple[p.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val types = getTypes[p.MirroredElemTypes]

    val arrowFields = labels.zip(types)
      .map((name, arrowType) =>
        arrowType match {
          case (true, tpe) => new Field(name, FieldType.nullable(tpe), null)
          case tpe: ArrowType => new Field(name, FieldType.notNullable(tpe), null)
          case (false, t) => throw new IllegalArgumentException(s"Wrong Option data type mapping: $t")
        }
      )

    new Schema(arrowFields.asJava)
  }

  private inline def getTypes[T <: Tuple]: List[ArrowType | (Boolean, ArrowType)] =
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (head *: tail) => arrowType[head] :: getTypes[tail]
    }

  private inline def arrowType[T]: ArrowType | (Boolean, ArrowType) =
    inline erasedValue[T] match {
      case _: Int => Types.MinorType.INT.getType
      case _: Long => Types.MinorType.BIGINT.getType
      case _: String => Types.MinorType.VARCHAR.getType
      case _: Double => Types.MinorType.FLOAT8.getType
      case _: Float => Types.MinorType.FLOAT4.getType
      case _: Boolean => Types.MinorType.BIT.getType
      case _: Array[Byte] => Types.MinorType.VARBINARY.getType
      case _: Instant => Types.MinorType.TIMESTAMPMILLI.getType
      case _: Seq[t] => new ArrowType.List()
      case _: Option[t] =>
        val tpe = arrowType[t] match
          case t: ArrowType => t
          case _ => throw new IllegalArgumentException(s"Unsupported type inside Option")
        (true, tpe)
      case t => throw new IllegalArgumentException(s"Unsupported type: $t")
    }
}

object ArrowSchema extends ArrowSchema