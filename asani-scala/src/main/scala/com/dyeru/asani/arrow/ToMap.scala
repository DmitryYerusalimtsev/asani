package com.dyeru.asani.arrow

import scala.deriving.*
import scala.compiletime.{constValueTuple, erasedValue, summonInline}

extension [T](value: T)(using instance: ToMap[T])
  def toMap: Map[String, Any] = instance.toMap(value)

trait ToMap[T] {
  def toMap(value: T): Map[String, Any]
}

object ToMap {
  def apply[T](using toMap: ToMap[T]): ToMap[T] = toMap

  inline def derived[T](using m: Mirror.Of[T]): ToMap[T] =
    inline m match {
      case p: Mirror.ProductOf[T] => (value: T) =>
        val labels = constValueTuple[p.MirroredElemLabels].productIterator.toList.map(_.toString)
        val values = value.asInstanceOf[Product].productIterator.toList
        labels.zip(values).toMap
    }

  // Enable derivation for case classes
  inline given derivedToMap[T](using m: Mirror.Of[T]): ToMap[T] = derived
}
