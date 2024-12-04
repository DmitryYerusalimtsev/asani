# asani
Lightning-fast IPC communication framework based on Apache Arrow and gRPC.

There are 2 implementation planned: 
- IPC using Apache Arrow memory-mapped files
- RPC using Arrow Flight

## Scala

### Server:

``` scala

import com.dyeru.asani.arrow.flight.{Server, FlightProcessor}

case class Person(name: String, age: Int)
case class PersonEnriched(name: String, age: Int, enriched: Boolean)

// Create a simple processor that echoes input data
val processor = new FlightProcessor[Person, PersonEnriched] {
    def command: String = "ENRICH"

    def process(in: List[Person]): List[PersonEnriched] = in.map(p => PersonEnriched(p.name, p.age, true))
}

// Define the test server
val server = new Server(List(processor))

server.start("localhost", 47470)

```

### Client:

``` scala

import com.dyeru.asani.arrow.flight.Client
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

given ExecutionContext = ExecutionContext.global

val client = Client(host, port)
val response = client.call[Person, PersonEnriched]("ENRICH", data)
val result = Await.result(response, 3.seconds)

```

## Python

### Server:

``` python

from typing import List

from pydantic import BaseModel

from asani.arrow.flight.flight_processor import FlightProcessor
from asani.arrow.flight.server import AsaniFlightServer


class Frame(BaseModel):
    streamName: str
    image: bytes


class Detection(BaseModel):
    stream: str
    label: str
    score: float
    bbox: list[float]


class ObjectTrackingProcessor(FlightProcessor[Frame, Detection]):
    def __init__(self):
        super().__init__(request_model=Frame, response_model=Detection)

    def command(self) -> str:
        return "object_tracking"

    def process(self, frames: List[Frame]) -> List[Detection]:
        detections = []
        for frame in frames:
            detections.append(Detection(
                stream=frame.streamName,
                label="person 1",
                score=57.0,
                bbox=[12, 31.2, 54.1, 45.2]
            ))
        return detections


if __name__ == "__main__":
    location = "grpc://localhost:8815"

    tracking_processor = ObjectTrackingProcessor()

    server = AsaniFlightServer(location=location, processors=[tracking_processor])

    server.serve()


```

### Client:

``` python

import pyarrow.flight as flight
from pydantic import BaseModel
import asyncio

from asani.arrow.flight.client import AsaniFlightClient

client = AsaniFlightClient(host="localhost", port=8815, request_model=Frame, response_model=Detection)

sample_data = [
    Frame(streamName='StreamA', image=b'VGhpcyBpcyBzYW1wbGUgaW1hZ2UgMQ=='),
    Frame(streamName='StreamB', image=b'VGhpcyBpcyBzYW1wbGUgaW1hZ2UgMg=='),
    Frame(streamName='StreamC', image=b'VGhpcyBpcyBzYW1wbGUgaW1hZ2UgMw==')
]

# Connect to the server using the client
response = asyncio.run(client.call("command_b", sample_data))

```
