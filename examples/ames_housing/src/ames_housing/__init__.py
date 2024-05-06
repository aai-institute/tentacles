"""Ames housing price prediction model training pipeline."""

from dagster import Definitions

from ames_housing.assets.ames_housing_data import ames_housing_data
from ames_housing.assets.ames_housing_features import ames_housing_features
from ames_housing.assets.price_prediction_models import (
    price_prediction_gradient_boosting_model,
    price_prediction_linear_regression_model,
    price_prediction_random_forest_model,
)
from ames_housing.assets.train_test import train_test_data
from ames_housing.constants import (
    AMES_HOUSING_DATA_SET_SEPARATOR,
    AMES_HOUSING_DATA_SET_URL,
)
from ames_housing.resources.csv_data_set_loader import CSVDataSetLoader

definitions = Definitions(
    assets=[
        ames_housing_data,
        ames_housing_features,
        train_test_data,
        price_prediction_linear_regression_model,
        price_prediction_random_forest_model,
        price_prediction_gradient_boosting_model,
    ],
    resources={
        "data_set_downloader": CSVDataSetLoader(
            path_or_url=AMES_HOUSING_DATA_SET_URL,
            separator=AMES_HOUSING_DATA_SET_SEPARATOR,
        ),
    },
)
