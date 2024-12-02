from datetime import datetime
from typing import List

import pyarrow.flight as flight
import pytest
from pydantic import BaseModel

from asani.arrow.flight.flight_processor import FlightProcessor
from asani.arrow.flight.server import AsaniFlightServer
from asani.arrow.serializer import Serializer


# Sample Pydantic model for testing
class Person(BaseModel):
    name: str
    age: int
    birthday: datetime


# Define mock processors
class MockProcessor(FlightProcessor[Person, Person]):
    def __init__(self, command_name):
        super().__init__(request_model=Person, response_model=Person)
        self._command_name = command_name

    def command(self) -> str:
        return self._command_name

    def process(self, models: List[Person]) -> List[Person]:
        # Mock processing logic
        return models


# Integration test
def test_asani_flight_server():
    # Create a mock server location
    location = "grpc://localhost:8815"

    # Define mock processors
    processor_a = MockProcessor(command_name="command_a")
    processor_b = MockProcessor(command_name="command_b")

    # Create the server
    server = AsaniFlightServer(location=location, processors=[processor_a, processor_b])

    # Mock a Flight client
    client = flight.FlightClient(location)

    # Start the server in a thread
    import threading

    server_thread = threading.Thread(target=server.serve, daemon=True)
    server_thread.start()

    try:
        # Connect to the server using the client
        descriptor = flight.FlightDescriptor.for_command("command_a")
        writer, reader = client.do_exchange(descriptor)

        # Sample test data
        sample_data = [
            Person(name="Alice", age=30, birthday=datetime(1990, 1, 1)),
            Person(name="Bob", age=25, birthday=datetime(1995, 5, 15)),
        ]

        ser = Serializer[Person](Person)
        root = ser.to_table(sample_data)

        # Send a request (can send Arrow batches if required)
        writer.begin(root.schema)
        writer.write_table(root)
        writer.done_writing()

        # Read response from server
        response_table = reader.read_all()

        response = Serializer[Person](Person).from_table(response_table)

        # Assert the response
        assert len(response) == 2
        assert response[0].name == sample_data[0].name
        assert response[0].age == sample_data[0].age
        assert response[0].birthday == sample_data[0].birthday
        assert response[1].name == sample_data[1].name
        assert response[1].age == sample_data[1].age
        assert response[1].birthday == sample_data[1].birthday
    finally:
        # Shutdown the server
        server.shutdown()
        server_thread.join()


if __name__ == "__main__":
    pytest.main([__file__])
