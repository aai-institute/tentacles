"""MLflow session resource."""

import os
from typing import Optional

import mlflow
from dagster import AssetExecutionContext, ConfigurableResource, InitResourceContext

from tentacles.utils.dagster import get_asset_key, get_run_id


class MlflowSession(ConfigurableResource):
    """MLflow session resource.

    Attributes
    ----------
    tracking_url : str
        URL of the MLflow tracking server.
    username : Optional[str]
        Optional username for authenticating against the MLflow tracking server.
    password : Optional[str]
        Optional password for authenticating against the MLflow tracking server.
    experiment : str
        Experiment name.
    use_asset_run_key : bool
        Whether the Dagster asset key should be included in the MLflow run name.
    use_dagster_run_id : bool
        Whether the Dagster run ID should be included in the MLflow run name.
    run_name_prefix : Optional[str]
        Optional prefix for the MLflow run name.
    """

    tracking_url: str
    username: Optional[str]
    password: Optional[str]
    experiment: str
    use_asset_run_key: bool = True
    use_dagster_run_id: bool = True
    run_name_prefix: Optional[str]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Setup the resource.

        Parameters
        ----------
        context : InitResourceContext
            Dagster context.
        """
        # mlflow expects the username and password as environment variables
        if self.username:
            os.environ["MLFLOW_TRACKING_USERNAME"] = self.username
        if self.password:
            os.environ["MLFLOW_TRACKING_PASSWORD"] = self.password

        mlflow.set_tracking_uri(self.tracking_url)
        mlflow.set_experiment(self.experiment)

    def _get_run_name_from_context(
        self, context: AssetExecutionContext, run_name_prefix: Optional[str]
    ) -> str:
        """Get the run name from the dagster context.

        The run name is constructed as follows:
        - The run name prefix (when provided)
        - The asset key name (if ``self.use_asset_run_key == True``)
        - The run identifier (if ``self.use_dagster_run_id == True``)

        Parameters
        ----------
        context : AssetExecutionContext
            Dagster context.
        run_name_prefix : Optional[str]
            Optional prefix to be added in front of the run name, default None

        Returns
        -------
        str
            Run name
        """

        if run_name_prefix is None:
            run_name_prefix = self.run_name_prefix

        parts: list[str] = []

        if self.use_asset_run_key:
            asset_key = get_asset_key(context)
            if isinstance(asset_key, list):
                raise ValueError("Can not derive MLflow run name for multi-assets.")

            parts.append(asset_key)

        if self.use_dagster_run_id:
            run_id = get_run_id(context, short=True)
            parts.append(run_id)

        if run_name_prefix is not None:
            parts.append(run_name_prefix)

        if len(parts) == 0:
            raise ValueError("Could not derive MLflow run name.")

        return "-".join(parts)

    def get_run(
        self,
        context: AssetExecutionContext,
        run_name_prefix: Optional[str] = None,
        tags: dict[str, str] = {},
    ) -> mlflow.ActiveRun:
        """Get the mlflow run.

        Parameters
        ----------
        context : AssetExecutionContext
            Dagster context
        run_name_prefix : str
            Optional prefix to the added in fron of the run name, default None
        tags : dict[str,str]
            Tags to be added to the run

        Notes
        -----
        MLflow only allows tags to be defined when starting a new run. Tags that are
        provided when re-starting an existing run will be ignored.
        """
        run_name = self._get_run_name_from_context(context, run_name_prefix)

        active_run = mlflow.active_run()
        if active_run is None:
            current_runs = mlflow.search_runs(
                filter_string=f"attributes.`run_name`='{run_name}'",
                output_format="list",
            )

            if current_runs:
                run_id = current_runs[0].info.run_id
                return mlflow.start_run(run_id=run_id, run_name=run_name)
            else:
                tags["dagster.run_id"] = get_run_id(context)
                tags["dagster.asset_name"] = get_asset_key(context)

                return mlflow.start_run(run_name=run_name, tags=tags)

        return active_run
