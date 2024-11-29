package com.dyeru.asani.arrow

import org.scalatest.funsuite.AnyFunSuite

// Sample case classes to test ToMap
case class Person(name: String, age: Int)
case class Employee(name: String, position: String, salary: Double)
case class Address(street: String, city: String, zip: String)

class ToMapTest extends AnyFunSuite {

  // Test case for Person case class
  test("toMap should convert Person case class to Map[String, Any]") {
    val person = Person("Alice", 30)
    val expectedMap = Map("name" -> "Alice", "age" -> 30)

    // Get the result of `toMap`
    val result = person.toMap

    assert(result == expectedMap)
  }

  // Test case for Employee case class
  test("toMap should convert Employee case class to Map[String, Any]") {
    val employee = Employee("Bob", "Developer", 100000.0)
    val expectedMap = Map("name" -> "Bob", "position" -> "Developer", "salary" -> 100000.0)

    // Get the result of `toMap`
    val result = employee.toMap

    assert(result == expectedMap)
  }

  // Test case for Address case class
  test("toMap should convert Address case class to Map[String, Any]") {
    val address = Address("123 Main St", "Metropolis", "12345")
    val expectedMap = Map("street" -> "123 Main St", "city" -> "Metropolis", "zip" -> "12345")

    // Get the result of `toMap`
    val result = address.toMap

    assert(result == expectedMap)
  }

  // Test case for empty case class (no fields)
  test("toMap should return an empty map for an empty case class") {
    case class Empty()
    val empty = Empty()
    val expectedMap = Map[String, Any]()

    val result = empty.toMap

    assert(result == expectedMap)
  }

  // Test case for a nested case class (case class inside a case class)
  test("toMap should handle nested case classes correctly") {
    val address = Address("123 Main St", "Metropolis", "12345")
    val employeeWithAddress = Employee("Bob", "Developer", 100000.0)
    
    // Nested case classes should be handled by their own ToMap instances
    val expectedMap = Map("name" -> "Bob", "position" -> "Developer", "salary" -> 100000.0)

    val result = employeeWithAddress.toMap

    assert(result == expectedMap)
  }
}