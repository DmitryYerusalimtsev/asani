import pytest
import pyarrow as pa
from datetime import datetime
from pydantic import BaseModel
from typing import List
from asani.arrow.serializer import Serializer


# Sample Pydantic model
class Person(BaseModel):
    name: str
    age: int
    birthday: datetime


# Initialize Serializer for Person
person_serializer = Serializer[Person](Person)


@pytest.fixture
def sample_data():
    return [
        Person(name="Alice", age=30, birthday=datetime(1990, 1, 1)),
        Person(name="Bob", age=25, birthday=datetime(1995, 5, 15)),
    ]


def test_from_table(sample_data):
    # Convert sample data to PyArrow Table
    arrow_table = person_serializer.to_table(sample_data)

    # Use the from_table method to convert back to Pydantic models
    result = person_serializer.from_table(arrow_table)

    # Test if the length of the result is correct
    assert len(result) == len(sample_data)

    # Test if the fields are correctly populated
    assert result[0].name == "Alice"
    assert result[1].age == 25
    assert result[1].birthday == datetime(1995, 5, 15)


def test_to_table(sample_data):
    # Convert sample data to PyArrow Table
    arrow_table = person_serializer.to_table(sample_data)

    # Check the schema of the resulting table
    assert arrow_table.schema.names == ["name", "age", "birthday"]

    # Check if the data types match
    assert arrow_table.schema.field("name").type == pa.string()
    assert arrow_table.schema.field("age").type == pa.int32()
    assert arrow_table.schema.field("birthday").type == pa.timestamp('ms')

    # Check if the data is correctly converted
    assert arrow_table.num_rows == len(sample_data)
    assert arrow_table.column("name").to_pylist() == ["Alice", "Bob"]
    assert arrow_table.column("age").to_pylist() == [30, 25]
    assert arrow_table.column("birthday").to_pylist() == [
        datetime(1990, 1, 1),
        datetime(1995, 5, 15)
    ]


def test_get_arrow_type():
    # Test supported field types
    assert person_serializer._get_arrow_type(int) == pa.int32()
    assert person_serializer._get_arrow_type(str) == pa.string()
    assert person_serializer._get_arrow_type(float) == pa.float64()
    assert person_serializer._get_arrow_type(bool) == pa.bool_()
    assert person_serializer._get_arrow_type(bytes) == pa.binary()
    assert person_serializer._get_arrow_type(datetime) == pa.timestamp('ms')

    # Test list type (e.g., list[int])
    assert person_serializer._get_arrow_type(List[int]) == pa.list_(pa.int32())

    # Test unsupported field type
    with pytest.raises(ValueError):
        person_serializer._get_arrow_type(dict)


if __name__ == "__main__":
    pytest.main()