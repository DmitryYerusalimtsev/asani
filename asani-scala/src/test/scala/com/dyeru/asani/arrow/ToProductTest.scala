package com.dyeru.asani.arrow

import com.dyeru.asani.arrow.createVectorSchemaRoot
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.arrow.vector.types.Types.MinorType

import java.time.Instant

class ToProductTest extends AnyFunSuite {

  test("ToProduct for case class with Int, String, Boolean") {
    case class TestClass(id: Int, name: String, active: Boolean)

    val fields = Seq(
      new Field("id", FieldType.nullable(MinorType.INT.getType), null),
      new Field("name", FieldType.nullable(MinorType.VARCHAR.getType), null),
      new Field("active", FieldType.nullable(MinorType.BIT.getType), null)
    )
    val data = Seq(
      Seq(1, "Alice", true),
      Seq(2, "Bob", false)
    )
    val root = createVectorSchemaRoot(fields, data)

    val result: List[TestClass] = root.toProducts

    assert(result == List(TestClass(1, "Alice", true), TestClass(2, "Bob", false)))

    root.close()
  }

  test("ToProduct for case class with Double and Instant") {
    case class TestClass(score: Double, timestamp: Instant)

    val fields = Seq(
      new Field("score", FieldType.nullable(MinorType.FLOAT8.getType), null),
      new Field("timestamp", FieldType.nullable(MinorType.TIMESTAMPMILLI.getType), null)
    )
    val data = Seq(
      Seq(95.5, Instant.ofEpochMilli(1633089600000L)),
      Seq(88.0, Instant.ofEpochMilli(1633176000000L))
    )
    val root = createVectorSchemaRoot(fields, data)

    val result: List[TestClass] = root.toProducts

    assert(result == List(
      TestClass(95.5, Instant.ofEpochMilli(1633089600000L)),
      TestClass(88.0, Instant.ofEpochMilli(1633176000000L))
    ))

    root.close()
  }

  test("ToProduct for case class with Array[Byte]") {
    case class TestClass(data: Array[Byte])

    val fields = Seq(
      new Field("data", FieldType.nullable(MinorType.VARBINARY.getType), null)
    )
    val data = Seq(
      Seq(Array[Byte](1, 2, 3)),
      Seq(Array[Byte](4, 5, 6))
    )
    val root = createVectorSchemaRoot(fields, data)

    val result: List[TestClass] = root.toProducts

    assert(result.map(_.data.toList) == List(List(1, 2, 3), List(4, 5, 6)))

    root.close()
  }

  test("ToProduct for case class with empty result") {
    case class TestClass(data: Array[Byte])

    val fields = Seq(
      new Field("data", FieldType.nullable(MinorType.VARBINARY.getType), null)
    )
    val data = Seq()
    val root = createVectorSchemaRoot(fields, data)

    val result: List[TestClass] = root.toProducts

    assert(result == List())

    root.close()
  }

  test("ToProduct for case class with Seq[Float]") {
    case class TestClass(data: Seq[Float])

    val data = Seq(
      TestClass(Seq[Float](1, 2, 3)),
      TestClass(Seq[Float](4, 5, 6))
    )

    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(ArrowSchema.derived[TestClass].schema, allocator)

    val vectorRoot = data.toArrowVector(root)

    val result: List[TestClass] = root.toProducts

    assert(result.map(_.data.toList) == List(List(1, 2, 3), List(4, 5, 6)))

    root.close()
  }

  test("ToProduct for case class with Seq[Option[Float]]") {
    case class TestClass(data: Seq[Option[Float]])

    val data = Seq(
      TestClass(Seq(None, Some(2), Some(3))),
      TestClass(Seq(Some(4), None, None))
    )

    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(ArrowSchema.derived[TestClass].schema, allocator)

    data.toArrowVector(root)

    val result: List[TestClass] = root.toProducts

    assert(result == List(
      TestClass(Seq(None, Some(2), Some(3))),
      TestClass(Seq(Some(4), None, None)))
    )

    root.close()
  }
}