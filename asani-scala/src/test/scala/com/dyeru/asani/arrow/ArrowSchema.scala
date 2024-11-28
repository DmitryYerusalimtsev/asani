package com.dyeru.asani.arrow

import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}
import org.apache.arrow.vector.types.Types
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArrowSchemaTest extends AnyFunSuite with Matchers {

  // A concrete trait to enable testing
  object ArrowSchemaTestImpl extends ArrowSchema

  test("schema should handle int type") {
    case class TestCase(id: Int)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "id"
    field.getType shouldBe Types.MinorType.INT.getType
  }

  test("schema should handle long type") {
    case class TestCase(id: Long)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "id"
    field.getType shouldBe Types.MinorType.BIGINT.getType
  }

  test("schema should handle String type") {
    case class TestCase(name: String)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "name"
    field.getType shouldBe Types.MinorType.VARCHAR.getType
  }

  test("schema should handle double type") {
    case class TestCase(value: Double)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "value"
    field.getType shouldBe Types.MinorType.FLOAT8.getType
  }

  test("schema should handle float type") {
    case class TestCase(value: Float)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "value"
    field.getType shouldBe Types.MinorType.FLOAT4.getType
  }

  test("schema should handle boolean type") {
    case class TestCase(flag: Boolean)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "flag"
    field.getType shouldBe Types.MinorType.BIT.getType
  }

  test("schema should handle byte array type") {
    case class TestCase(data: Array[Byte])

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "data"
    field.getType shouldBe Types.MinorType.VARBINARY.getType
  }

  test("schema should handle Instant type") {
    case class TestCase(timestamp: java.time.Instant)

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "timestamp"
    field.getType shouldBe Types.MinorType.TIMESTAMPMILLI.getType
  }

  test("schema should handle Option[type] correctly") {
    case class TestCase(value: Option[Int])

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "value"
    field.isNullable shouldBe true
    field.getType shouldBe Types.MinorType.INT.getType
  }

  test("schema should handle List[type] correctly") {
    case class TestCase(values: List[String])

    val schema: Schema = ArrowSchemaTestImpl.schema[TestCase]

    val field = schema.getFields.get(0)
    field.getName shouldBe "values"
    field.isNullable shouldBe false
    field.getType shouldBe Types.MinorType.VARCHAR.getType
  }

}