from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List
import pyarrow as pa
from pydantic import BaseModel
from asani.arrow.serializer import Serializer
from asani.processor import Processor

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)


class FlightProcessor(ABC, Generic[In, Out], Processor[In, Out]):

    def __init__(self):
        super().__init__()  # Use super to ensure proper initialization
        self.request_serializer: Serializer[In] = Serializer[In]
        self.response_serializer: Serializer[Out] = Serializer[Out]

    def process_request(self, reader, writer):
        models = []

        root = reader.get_root()
        while reader.next():
            try:
                table = pa.Table.from_batches([root.to_pandas()])
                models = self.request_serializer.from_table(table)
            except Exception as e:
                print(f"Error parsing records to model: {root}, error: {e}")

        result = self.process(models)
        response = self.response_serializer.to_table(result)

        writer.begin(response.schema)
        writer.write_batches(response.to_batches())
        writer.end()

    @abstractmethod
    def command(self) -> str:
        raise NotImplemented()

    @abstractmethod
    def process(self, models: List[In]) -> List[Out]:
        raise NotImplemented()
