"""Dagster utilities."""

from dagster import AssetExecutionContext

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


def get_asset_key(context: AssetExecutionContext) -> str:
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
    return context.asset_key.to_user_string()
