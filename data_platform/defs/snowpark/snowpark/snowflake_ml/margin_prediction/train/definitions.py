import dagster as dg

from .......utils.automation_conditions import CustomAutomationCondition
from .....resources import SnowparkResource
from .model import materialze


class MarginPredictionConfig(dg.Config):
    """Exposes configuration options to end users in the Dagster launchpad."""
    retrain_treshold: float = 0.7


@dg.asset(
        key=["ml", "model", "margin_prediction"],
        deps=[["transaction_db", "stg", "transactions"]],
        kinds={"snowpark", "xgboost"},
        group_name="data_science",
        description="Toy model used to predict the margin of transactions.",
        automation_condition=CustomAutomationCondition.on_cron("@daily")
)
def asset(
        context: dg.AssetExecutionContext,
        snowpark: SnowparkResource,
        config: MarginPredictionConfig) -> dg.MaterializeResult:

    session = snowpark.get_session(schema="transaction_db")
    metadata = materialze(context, session, config.retrain_treshold)

    return dg.MaterializeResult(metadata=metadata)

@dg.asset_check(
        asset=["ml", "model", "margin_prediction"],
        description=("Check that default version of model scores above"
                     "threshold for retraining")
)
def score_above_threshold_check(
        snowpark: SnowparkResource,
        config: MarginPredictionConfig) -> dg.AssetCheckResult:
    
    from snowflake.ml.registry.registry import Registry
    session = snowpark.get_session(schema="transaction_db")
    registry = Registry(session)
    model = registry.get_model("margin_prediction").default
    score = model.get_metric("score")
    return dg.AssetCheckResult(
        passed=bool(score > config.retrain_treshold),
        metadata={"score": score}
    )
