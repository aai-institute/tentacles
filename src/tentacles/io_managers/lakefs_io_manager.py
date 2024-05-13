"""LakeFS IO manager."""

from pathlib import PosixPath
from typing import Any, Optional, Union

from dagster import ConfigurableIOManager, InputContext, OutputContext
from lakefs_spec.spec import LakeFSFileSystem
from lakefs_spec.transaction import LakeFSTransaction

from tentacles.io_managers.serializers.serializer import Serializer
from tentacles.utils.dagster import get_metadata


class LakeFSIOManager(ConfigurableIOManager):
    """Base lakeFS IO manager.

    This this IO manager as a basis for creating specialized IO managers that serialize
    objects to a lakeFS versioned data lake.

    Attributes
    ----------
    extension : str
        File extension.
    serializer : Serializer
        Serializer that converts the Python object to a file, and vice versa.
    repository : str
        Repository on the LakeFS server
    branch : str
        Branch in the repository
    """

    extension: str
    serializer: Serializer

    repository: str
    branch: str

    def get_path(
        self,
        context: Union[OutputContext, InputContext],
        transaction: Optional[LakeFSTransaction] = None,
        commit_id: Optional[str] = None,
    ) -> str:
        """Get path in lakeFS based on the asset key.

        If a transaction is provided, the temporary branch created for the transaction
        will be used instead of the branch name provided as part of the asset key.

        If a commit identifier is provided, the branch name will be replaced with the
        commit id.

        Parameters
        ----------
        context : Union[OutputContext, InputContext]
            Dagster context
        transaction : Optional[LakeFSTransaction]
            lakeFS-spec transaction, by default None

        Returns
        -------
        str
            Path to the object in lakeFS.
        """

        metadata = get_metadata(context)

        print(f"Metadata: {metadata.get('path')}")
        print(f"Asset key path: {context.asset_key.path}")
        
        path = PosixPath(*(metadata.get("path") + context.asset_key.path))

        if transaction is not None:
            branch = transaction.branch.id
        elif commit_id is not None:
            branch = commit_id
        else:
            branch = self.branch

        return f"lakefs://{self.repository}/{branch}/{path}{self.extension}"

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Serialize the Python object to an object in lakeFS.

        Parameters
        ----------
        context : OutputContext
            Dagster context.
        obj : Any
            Python objec that will be serialized to an object in lakeFS.
        """

        fs = LakeFSFileSystem()

        with fs.transaction(repository=self.repository, base_branch=self.branch) as tx:
            with fs.open(self.get_path(context, transaction=tx), "wb") as f:
                context.log.debug(f"Writing file at: {self.get_path(context)}")
                self.serializer.serialize(f, obj)

            asset_name = PosixPath(*context.asset_key.path)
            commit = tx.commit(message=f"Add asset {asset_name}")

        context.add_output_metadata(
            {
                "lakefs_commit": commit.id,
                "lakefs_url": self.get_path(context),
                "lakefs_permalink": self.get_path(context, commit_id=commit.id),
            }
        )

    def load_input(self, context: InputContext) -> Any:
        """Load file contects into Python object.

        Parameters
        ----------
        context : InputContext
            Dagster context.

        Returns
        -------
        Object with the contents of the file in lakeFS.
        """

        fs = LakeFSFileSystem()

        path = self.get_path(context)
        with fs.open(path, "r") as f:
            result = self.serializer.deserialize(f)
        return result
