"""Ames housing data set."""

import pandas as pd
from caseconverter import snakecase
from dagster import asset

from ames_housing.resources.csv_data_set_loader import CSVDataSetLoader


@asset(group_name="ingestion", compute_kind="pandas", io_manager_key="csv_io_manager")
def ames_housing_data(
    data_set_downloader: CSVDataSetLoader,
) -> pd.DataFrame:
    """Ames housing data set.

    Parameters
    ----------
    data_set_downloader : CSVDataSetLoader
        Raw data set loader.

    Returns
    -------
    pd.DataFrame
        Raw Ames housing data set."""
    raw_data_df = data_set_downloader.load()

    # Re-format column names to snake case. The original data set uses several
    # different formats for its column names.
    raw_data_df = raw_data_df.rename(mapper=snakecase, axis=1)

    return raw_data_df
