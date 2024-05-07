"""Pickle serializer."""

from typing import Any

import joblib

from tentacles.io_managers.serializers.serializer import Serializer


class PickleSerializer(Serializer):
    """Pickle serializer.

    Serializes Python objects to pickle files using joblib.
    """

    def serialize(self, f: Any, obj: Any) -> None:
        """Write object to pickle file."""
        joblib.dump(obj, f)

    def deserialize(self, f: Any) -> Any:
        """Read object from pickle file."""
        return joblib.load(f)
