"""Filesystem IO manager."""

import os
from typing import Any, Union

from dagster import ConfigurableIOManager, InputContext, OutputContext

from tentacles.io_managers.serializers.serializer import Serializer


class FilesystemIOManager(ConfigurableIOManager):
    """Filesystem IO manager.

    This IO manager serializes assets as files on the (local) file system.

    Attributes
    ----------
    base_dir : str
        Directory where the assets are written to/read from.
    extension : str
        File extension.
    serializer : Serializer
        Serializer that converts the Python object to a file, and vice versa.
    """

    base_dir: str
    extension: str
    serializer: Serializer

    def _get_path(self, context: Union[OutputContext, InputContext]) -> str:
        """Get the path to the asset on the local file system.

        Parameters
        ----------
        context : Union[OutputContext, InputContext]
            Dagster context.

        Returns
        -------
        str
            Path to the asset.
        """
        return self.base_dir + "/" + "/".join(context.asset_key.path) + self.extension

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Write the provided object to a file on the file system.

        Parameters
        ----------
        context : OutputContext
            Dagster context.
        obj : Any
            Object to be serialized
        """
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        context.log.debug(f"Writing file at: {path}")
        with open(path, "wb") as f:
            self.serializer.serialize(f, obj)

    def load_input(self, context: InputContext) -> Any:
        """Read the asset from the file system into an object.

        Parameters
        ----------
        context : InputContext
            Dagster context.
        """
        path = self._get_path(context)

        context.log.debug(f"Loading file from: {path}")
        with open(path, "rb") as f:
            return self.serializer.deserialize(f)
