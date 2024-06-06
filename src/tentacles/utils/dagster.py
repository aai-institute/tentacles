"""Dagster utilities."""

from typing import Union

from dagster import AssetExecutionContext, InputContext, OutputContext

SHORT_RUN_ID_LENGTH = 8
"""Length of the short version of the run id."""


def get_run_id(context: AssetExecutionContext, short: bool = False) -> str:
    """Get run id from the dagster context.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster context.
    short : bool
        Get only the beginning of the run id, similar to how it is shown in the user
        interface.

    Returns
    -------
    str
        Run id
    """
    run_id = context.run.run_id
    return run_id[:SHORT_RUN_ID_LENGTH] if short else run_id


def get_asset_key(context: AssetExecutionContext) -> str | list[str]:
    """Get the asset key from the dagster context.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster context.

    Returns
    -------
    str
        Asset key.
    """
    outputs = context.selected_output_names
    if len(outputs) == 1:
        return context.asset_key.to_user_string()
    return [
        context.asset_key_for_output(o).to_user_string()
        for o in outputs
    ]


def get_metadata(context: Union[OutputContext, InputContext]) -> dict:
    """Get metadata from the dagster context.

    Parameters
    ----------
    context : Union[OutputContext, InputContext]
        Dagster context.

    Returns
    -------
    dict
        Asset metadata
    """
    if isinstance(context, OutputContext):
        return context.metadata
    else:  # type is InputContext
        return context.upstream_output.metadata
