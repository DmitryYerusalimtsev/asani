import pytest
from unittest.mock import MagicMock
import pyarrow as pa
from datetime import datetime
from pydantic import BaseModel
from typing import List

from asani.arrow.flight.flight_processor import FlightProcessor
from asani.arrow.serializer import Serializer


# Sample Pydantic model for testing
class Person(BaseModel):
    name: str
    age: int
    birthday: datetime


# Sample test data
sample_data = [
    Person(name="Alice", age=30, birthday=datetime(1990, 1, 1)),
    Person(name="Bob", age=25, birthday=datetime(1995, 5, 15)),
]


# Mocking Serializer methods
class MockSerializer(Serializer[Person]):
    def __init__(self):
        super().__init__(Person)

    def from_table(self, table: pa.Table) -> List[Person]:
        # Mock behavior: directly return the sample data
        return sample_data

    def to_table(self, data: List[Person]) -> pa.Table:
        # Mock behavior: create a table with the sample data
        return pa.table([
            pa.array([person.name for person in data], type=pa.string()),
            pa.array([person.age for person in data], type=pa.int32()),
            pa.array([person.birthday for person in data], type=pa.timestamp('ms')),
        ], schema=pa.schema([
            ('name', pa.string()),
            ('age', pa.int32()),
            ('birthday', pa.timestamp('ms'))
        ]))


# Test FlightProcessor with mocked reader and writer
class TestFlightProcessor(FlightProcessor[Person, Person]):
    def __init__(self):
        super().__init__()

    def command(self) -> str:
        raise NotImplemented()

    def process(self, models: List[Person]) -> List[Person]:
        # Mock process function: Just return the models as they are
        return models


@pytest.fixture
def flight_processor():
    return TestFlightProcessor()


@pytest.fixture
def mock_reader():
    reader = MagicMock()
    # Mock the get_root method to return a Table
    table = pa.table([
        pa.array([person.name for person in sample_data], type=pa.string()),
        pa.array([person.age for person in sample_data], type=pa.int32()),
        pa.array([person.birthday for person in sample_data], type=pa.timestamp('ms')),
    ], schema=pa.schema([
        ('name', pa.string()),
        ('age', pa.int32()),
        ('birthday', pa.timestamp('ms'))
    ]))
    root = MagicMock()
    root.to_pandas.return_value = table.to_pandas()
    reader.get_root.return_value = root
    # Simulate `next` method to iterate through batches
    reader.next.return_value = True
    return reader


@pytest.fixture
def mock_writer():
    return MagicMock()


def test_process_request(flight_processor, mock_reader, mock_writer):
    # Call the process_request method to test it
    flight_processor.process_request(mock_reader, mock_writer)

    # Check that the serializer's from_table method was called
    flight_processor.request_serializer.from_table.assert_called_once()

    # Check that the process method was called with the models
    flight_processor.process.assert_called_once_with(sample_data)

    # Check that the response serializer's to_table method was called
    flight_processor.response_serializer.to_table.assert_called_once()

    # Check that the writer's methods were called
    mock_writer.begin.assert_called_once()
    mock_writer.write_batches.assert_called_once()
    mock_writer.end.assert_called_once()

    # Ensure that we have correctly processed and written the batches
    assert mock_writer.write_batches.call_args[0][0].num_rows == len(sample_data)


def test_invalid_data(flight_processor, mock_reader, mock_writer):
    # Simulate a reader that raises an exception
    mock_reader.next.return_value = False  # Simulate no data

    # Call process_request to test error handling
    flight_processor.process_request(mock_reader, mock_writer)

    # Check if any error was logged (in production code, you might use a logger)
    # Here we will simply ensure that no errors cause the process to crash.
    assert mock_writer.write_batches.call_count == 0
    mock_writer.begin.assert_called_once()


if __name__ == "__main__":
    pytest.main()