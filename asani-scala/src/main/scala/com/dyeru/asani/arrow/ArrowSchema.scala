package com.dyeru.asani.arrow

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.Types

import scala.compiletime.{constValueTuple, erasedValue, summonInline}
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters.*

trait ArrowSchema {
  inline def schema[A](using p: Mirror.ProductOf[A]): Schema = {
    val labels = constValueTuple[p.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val types = getTypes[p.MirroredElemTypes]
    val fields = labels.zip(types)
    val optionRegex = genericTypeRegex("Option")

    val arrowFields = fields
      .map((name, tpe) =>
        tpe match {
          case optionRegex(innerType) => new Field(name, FieldType.nullable(getType(innerType)), null)
          case topType => new Field(name, FieldType.notNullable(getType(topType)), null)
        }
      )

    new Schema(arrowFields.asJava)
  }

  private def getType(tpe: String): ArrowType = tpe match {
    case "int" => Types.MinorType.INT.getType
    case "long" => Types.MinorType.BIGINT.getType
    case "String" => Types.MinorType.VARCHAR.getType
    case "double" => Types.MinorType.FLOAT8.getType
    case "float" => Types.MinorType.FLOAT4.getType
    case "boolean" => Types.MinorType.BIT.getType
    case "byte[]" => Types.MinorType.VARBINARY.getType
    case "Instant" => Types.MinorType.TIMESTAMPMILLI.getType
    case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
  }

  private inline def getTypes[T <: Tuple]: List[String] =
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (head *: tail) => typeName[head] :: getTypes[tail]
    }

  private inline def typeName[T]: String =
    inline erasedValue[T] match {
      case _: Option[t] => s"Option[${typeName[t]}]"
      case _: Seq[t] => s"List[${typeName[t]}]"
      case _ => summonInline[ClassTag[T]].runtimeClass.getSimpleName
    }

  private inline def genericTypeRegex(topType: String) = (topType + """\[(\w+)]""").r
}
