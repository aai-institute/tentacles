"""Price prediction models."""

import mlflow
import pandas as pd
from dagster import AssetExecutionContext, asset
from sklearn.pipeline import Pipeline
from tentacles.resources.mlflow_session import MlflowSession

from ames_housing.constants import LAKEFS_MODEL_PATH, TARGET
from ames_housing.model_factory import ModelFactory


def _fit_and_score_pipeline(
    context: AssetExecutionContext,
    mlflow_session: MlflowSession,
    pipeline: Pipeline,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
) -> Pipeline:
    """Fit and score specified pipeline.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster context.
    pipeline : Pipeline
        Pipeline definition.
    train_data : pd.DataFrame
        Training data set.
    test_data : pd.DataFrame
        Test data set.

    Returns
    -------
    Pipeline
        Fitted pipeline.
    """

    with mlflow_session.get_run(context, run_name_prefix="ames_housing"):
        mlflow.sklearn.autolog()

        pipeline.fit(
            train_data.drop([TARGET], axis=1),
            train_data[TARGET],
        )

        score = pipeline.score(
            test_data,
            test_data[TARGET],
        )
        context.log.info(f"Score: {score}")

    return pipeline


@asset(
    group_name="training",
    compute_kind="scikitlearn",
    io_manager_key="model_io_manager",
    metadata={"path": LAKEFS_MODEL_PATH},
)
def linear_regression_model(
    context: AssetExecutionContext,
    mlflow_session: MlflowSession,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
) -> Pipeline:
    """Price prediction linear regression model."""
    return _fit_and_score_pipeline(
        context,
        mlflow_session,
        ModelFactory.create_linear_regression_pipeline(),
        train_data,
        test_data,
    )


@asset(
    group_name="training",
    compute_kind="scikitlearn",
    io_manager_key="model_io_manager",
    metadata={"path": LAKEFS_MODEL_PATH},
)
def random_forest_model(
    context: AssetExecutionContext,
    mlflow_session: MlflowSession,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
) -> Pipeline:
    """Price prediction random forest regressor model."""
    return _fit_and_score_pipeline(
        context,
        mlflow_session,
        ModelFactory.create_random_forest_pipeline(),
        train_data,
        test_data,
    )


@asset(
    group_name="training",
    compute_kind="scikitlearn",
    io_manager_key="model_io_manager",
    metadata={"path": LAKEFS_MODEL_PATH},
)
def gradient_boosting_model(
    context: AssetExecutionContext,
    mlflow_session: MlflowSession,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
) -> Pipeline:
    """Price prediction gradient boosting regressor model."""
    return _fit_and_score_pipeline(
        context,
        mlflow_session,
        ModelFactory.create_gradient_boosting_regressor_pipeline(),
        train_data,
        test_data,
    )
