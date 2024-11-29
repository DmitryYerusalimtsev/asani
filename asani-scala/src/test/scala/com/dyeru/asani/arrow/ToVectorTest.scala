package com.dyeru.asani.arrow

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.InvalidArrowFileException
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.funsuite.AnyFunSuite

import scala.runtime.stdLibPatches.Predef.assert

case class EmployeeWithAddress(name: String, position: String, address: Address)

class ToVectorTest extends AnyFunSuite {

  // Test case for Person case class
  test("toArrowVector should convert Person case class to Arrow Vector") {
    val people = Seq(Person("Alice", 30), Person("Bob", 25))

    val vectorRoot = people.toArrowVector

    // Assertions to check if the Arrow Vector contains expected values
    val nameVector = vectorRoot.getVector("name").asInstanceOf[VarCharVector]
    assert(new String(nameVector.get(0)) == "Alice")
    assert(new String(nameVector.get(1)) == "Bob")

    val ageVector = vectorRoot.getVector("age").asInstanceOf[IntVector]
    assert(ageVector.get(0) == 30)
    assert(ageVector.get(1) == 25)

    vectorRoot.close()
  }

  // Test case for Employee case class
  test("toArrowVector should convert Employee case class to Arrow Vector") {
    val employees = Seq(Employee("Bob", "Developer", 100000.0), Employee("Alice", "Manager", 120000.0))

    val vectorRoot = employees.toArrowVector

    val nameVector = vectorRoot.getVector("name").asInstanceOf[VarCharVector]
    assert(new String(nameVector.get(0)) == "Bob")
    assert(new String(nameVector.get(1)) == "Alice")

    val positionVector = vectorRoot.getVector("position").asInstanceOf[VarCharVector]
    assert(new String(positionVector.get(0)) == "Developer")
    assert(new String(positionVector.get(1)) == "Manager")

    val salaryVector = vectorRoot.getVector("salary").asInstanceOf[Float8Vector]
    assert(salaryVector.get(0) == 100000.0)
    assert(salaryVector.get(1) == 120000.0)

    vectorRoot.close()
  }

  // Test case for Address case class
  test("toArrowVector should convert Address case class to Arrow Vector") {
    val addresses = Seq(Address("123 Main St", "Metropolis", "12345"), Address("456 Oak St", "Smalltown", "67890"))

    val vectorRoot = addresses.toArrowVector

    val streetVector = vectorRoot.getVector("street").asInstanceOf[VarCharVector]
    assert(new String(streetVector.get(0)) == "123 Main St")
    assert(new String(streetVector.get(1)) == "456 Oak St")

    val cityVector = vectorRoot.getVector("city").asInstanceOf[VarCharVector]
    assert(new String(cityVector.get(0)) == "Metropolis")
    assert(new String(cityVector.get(1)) == "Smalltown")

    val zipVector = vectorRoot.getVector("zip").asInstanceOf[VarCharVector]
    assert(new String(zipVector.get(0)) == "12345")
    assert(new String(zipVector.get(1)) == "67890")

    vectorRoot.close()
  }

  // Test case for empty list
  test("toArrowVector should return an empty vector for an empty list") {
    val emptyList = Seq.empty[Person]

    val vectorRoot = emptyList.toArrowVector

    assert(vectorRoot.getRowCount == 0)

    vectorRoot.close()
  }

  // Test case for Option type
  test("toArrowVector should handle Option type correctly") {
    case class PersonOpt(name: String, age: Option[Int])

    val people = Seq(PersonOpt("Alice", Some(30)), PersonOpt("Bob", None))

    val vectorRoot = people.toArrowVector

    val nameVector = vectorRoot.getVector("name").asInstanceOf[VarCharVector]
    assert(new String(nameVector.get(0)) == "Alice")
    assert(new String(nameVector.get(1)) == "Bob")

    // Handling the Option type for age
    val ageVector = vectorRoot.getVector("age").asInstanceOf[IntVector]
    assert(ageVector.get(0) == 30)
    assert(ageVector.isNull(1)) // Check that the second element is null (because age is None)

    vectorRoot.close()
  }

  // Test case for List[String] in a case class (no nesting)
  test("toArrowVector should handle List[String] correctly in a case class") {
    case class PersonWithNumbers(name: String, age: Int, phoneNumbers: List[String])

    // Sample data with List[String] field
    val people = Seq(
      PersonWithNumbers("Alice", 30, List("123-456-7890", "234-567-8901")),
      PersonWithNumbers("Bob", 25, List("987-654-3210"))
    )

    // Convert to Arrow Vector
    val vectorRoot = people.toArrowVector

    // Access the 'phoneNumbers' field vector which will be a ListVector
    val phoneNumbersVector = vectorRoot.getVector("phoneNumbers").asInstanceOf[ListVector]

    // Verify the value count (the number of rows)
    assert(phoneNumbersVector.getValueCount == 2)

    // Test first record: Alice (with 2 phone numbers)
    val firstPhoneNumbers = phoneNumbersVector.getObject(0).toArray.map(_.toString).toList
    assert(firstPhoneNumbers == List("123-456-7890", "234-567-8901"))

    // Test second record: Bob (with 1 phone number)
    val secondPhoneNumbers = phoneNumbersVector.getObject(1).toArray.map(_.toString).toList
    assert(secondPhoneNumbers == List("987-654-3210"))

    vectorRoot.close()
  }

  // Test case for Array[Byte] type
  test("toArrowVector should handle Array[Byte] correctly in a case class") {
    case class PersonWithBytes(name: String, data: Array[Byte])

    // Sample data with Array[Byte] field
    val people = Seq(
      PersonWithBytes("Alice", Array(1, 2, 3, 4)),
      PersonWithBytes("Bob", Array(5, 6, 7))
    )

    // Convert to Arrow Vector
    val vectorRoot = people.toArrowVector

    // Access the 'data' field vector which will be a ListVector containing VarBinary
    val dataVector = vectorRoot.getVector("data").asInstanceOf[VarBinaryVector]

    // Verify the value count (the number of rows)
    assert(dataVector.getValueCount == 2)

    // Test first record: Alice (with Array[Byte](1, 2, 3, 4))
    val firstData = dataVector.getObject(0)
    assert(firstData.sorted sameElements Array(1, 2, 3, 4).sorted)

    // Test second record: Bob (with Array[Byte](5, 6, 7))
    val secondData = dataVector.getObject(1)
    assert(secondData.sorted sameElements Array(5, 6, 7).sorted)

    vectorRoot.close()
  }
}