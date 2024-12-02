from datetime import datetime
from typing import Type, TypeVar, Generic, List
from pydantic import BaseModel
import pyarrow as pa

T = TypeVar("T", bound=BaseModel)


class Serializer(Generic[T]):
    def __init__(self, model: Type[T]):
        self.model = model

    def from_table(self, table: pa.Table) -> List[T]:
        """
        Convert a PyArrow Table (VectorSchemaRoot) to a list of Pydantic models.
        """
        records = table.to_pylist()  # Convert to list of dictionaries
        return [self.model(**record) for record in records]

    def to_table(self, data: List[T]) -> pa.Table:
        """
        Convert a list of Pydantic models to a PyArrow Table (VectorSchemaRoot).
        """
        # Get the schema (fields) from the Pydantic model
        schema = self.model.__annotations__

        # Prepare a list of Arrow Field Vectors
        vectors = []
        for field, field_type in schema.items():
            # Initialize an empty list to collect values for this field
            field_data = [getattr(item, field) for item in data]

            # Determine Arrow type based on the Pydantic model field type
            arrow_type = self._get_arrow_type(field_type)
            vector = pa.array(field_data, type=arrow_type)
            vectors.append(vector)

        # Create a schema based on the fields
        arrow_schema = pa.schema([
            pa.field(field, self._get_arrow_type(field_type)) for field, field_type in schema.items()
        ])

        # Create a Table from the vectors and schema
        return pa.table(vectors, schema=arrow_schema)

    def _get_arrow_type(self, field_type: Type) -> pa.DataType:
        if field_type == int:
            return pa.int32()
        elif field_type == int:
            return pa.int64()
        elif field_type == str:
            return pa.string()
        elif field_type == float:
            return pa.float64()
        elif field_type == float:
            return pa.float32()
        elif field_type == bool:
            return pa.bool_()
        elif field_type == bytes:
            return pa.binary()
        elif field_type == datetime:
            return pa.timestamp('ms')
        elif hasattr(field_type, '__origin__') and field_type.__origin__ == list:
            # For sequences (lists), map to Arrow list type
            element_type = field_type.__args__[0]
            return pa.list_(self._get_arrow_type(element_type))
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
