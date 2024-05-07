"""Base serializer."""

from typing import Any

from dagster import ConfigurableResource


class Serializer(ConfigurableResource):
    """Base serializer."""

    def serialize(self, f: Any, obj: Any) -> None:
        raise NotImplementedError()

    def deserialize(self, f: Any) -> Any:
        raise NotImplementedError()
