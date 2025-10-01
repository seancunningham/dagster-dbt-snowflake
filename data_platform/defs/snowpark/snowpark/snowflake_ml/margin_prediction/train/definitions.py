"""Machine learning example assets that train and validate a margin prediction model."""

import dagster as dg

from .......utils.automation_conditions import CustomAutomationCondition
from .....resources import SnowparkResource
from .model import materialze


class MarginPredictionConfig(dg.Config):
    """Expose margin prediction configuration options in the Dagster Launchpad."""
    retrain_treshold: float = 0.7


@dg.asset(
    key=["ml", "model", "margin_prediction"],
    deps=[["transaction_db", "stg", "transactions"]],
    kinds={"snowpark", "xgboost"},
    group_name="data_science",
    description="Toy model used to predict the margin of transactions.",
    automation_condition=CustomAutomationCondition.on_cron("@daily"),
)
def asset(
    context: dg.AssetExecutionContext,
    snowpark: SnowparkResource,
    config: MarginPredictionConfig,
) -> dg.MaterializeResult:
    """Train or reuse the margin prediction model depending on its validation score.

    Args:
        context: Dagster execution context used for logging metadata and events.
        snowpark: Snowpark resource providing an authenticated session to Snowflake.
        config: Runtime configuration indicating the retraining threshold.

    Returns:
        dagster.MaterializeResult: Result containing model metadata, including updated
        evaluation metrics emitted by the training helper.
    """

    session = snowpark.get_session(schema="transaction_db")
    metadata = materialze(context, session, config.retrain_treshold)

    return dg.MaterializeResult(metadata=metadata)

@dg.asset_check(
    asset=["ml", "model", "margin_prediction"],
    description=(
        "Check that default version of model scores above threshold for retraining"
    ),
)
def score_above_threshold_check(
    snowpark: SnowparkResource,
    config: MarginPredictionConfig,
) -> dg.AssetCheckResult:
    """Ensure the default registered model continues to meet the configured threshold.

    Args:
        snowpark: Snowpark resource used to query the Snowflake ML registry.
        config: Runtime configuration specifying the minimum acceptable score.

    Returns:
        dagster.AssetCheckResult: Check result containing the latest model score and a
        pass flag indicating whether retraining is necessary.
    """

    from snowflake.ml.registry.registry import Registry
    session = snowpark.get_session(schema="transaction_db")
    registry = Registry(session)
    model = registry.get_model("margin_prediction").default
    score = model.get_metric("score")
    return dg.AssetCheckResult(
        passed=bool(score > config.retrain_treshold),
        metadata={"score": score}
    )
