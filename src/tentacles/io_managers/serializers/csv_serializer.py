"""CSV serializer."""

from typing import Any

import pandas as pd

from tentacles.io_managers.serializers.serializer import Serializer


class CSVSerializer(Serializer):
    """CSV serializer.

    Serializes Panda's DataFrames to CSV files.

    Attributes
    ----------
    separator : str
        Field delimiter.
    """

    separator: str = ","

    def serialize(self, f: Any, obj: pd.DataFrame) -> None:
        """Write data frame to CSV file."""
        obj.to_csv(f, sep=self.separator)

    def deserialize(self, f: Any) -> pd.DataFrame:
        """Read CSV file into data frame."""
        return pd.read_csv(f, sep=self.separator)
