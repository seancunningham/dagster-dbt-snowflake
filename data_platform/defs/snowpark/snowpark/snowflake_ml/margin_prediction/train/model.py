"""Training utilities for the sample Snowflake ML margin prediction model."""

from typing import Any

import dagster as dg
from snowflake.ml.modeling.framework.base import BaseTransformer
from snowflake.ml.modeling.linear_model.linear_regression import LinearRegression
from snowflake.ml.modeling.pipeline.pipeline import Pipeline
from snowflake.ml.modeling.preprocessing.one_hot_encoder import OneHotEncoder
from snowflake.ml.modeling.preprocessing.standard_scaler import StandardScaler
from snowflake.ml.modeling.xgboost.xgb_regressor import XGBRegressor
from snowflake.ml.registry.registry import Registry
from snowflake.snowpark import functions as F
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session


def materialze(
    context: dg.AssetExecutionContext,
    session: Session,
    retrain_threshold: float,
) -> dict[str, Any]:
    """Execute the margin prediction model retraining pipeline.

    Args:
        context: Dagster execution context for logging decisions and metrics.
        session: Active Snowpark session used to query training data and the registry.
        retrain_threshold: Minimum acceptable validation score before retraining.

    Returns:
        dict[str, Any]: Metadata describing the selected model version and validation
        score after running the retraining logic.

    The pipeline performs the following steps:

    1. Retrieve the currently registered model version and score it on validation data.
    2. Skip retraining when the score exceeds ``retrain_threshold``.
    3. Otherwise train candidate models, evaluate each, and select the top performer.
    4. Register the improved model, promoting it to the default version when its score
       meets or exceeds the previous score.
    5. Surface the resulting version and score for downstream observability.
    """

    
    model_name = "margin_prediction"
    val = _get_validation_data(session)
    registry = Registry(session)
    try:
        model_ref = registry.get_model(model_name).default
        
    except Exception:
        model_ref = None
    
    if model_ref:
        context.log.info("previous version found, checking score.")
        model = model_ref.load() 
        old_version_name = model_ref.version_name
        old_score = model.score(val)
        model_ref.set_metric("score", old_score)
        if old_score >= retrain_threshold:
            context.log.info("Score above threshold, skipping retrain.")
            return {"version": model_ref.version_name, "score": old_score}
        else:
            context.log.info("Score below threshold, starting retrain.")

    else:
        context.log.info("No previous model version found.")
        old_score = 0
        old_version_name = "not_registered"

    context.log.info("Training model.")
    df = _get_train_data(session)
    model = _train_model(df, context)
    new_score = float(model.score(val)) # type: ignore

    if new_score >= old_score:
        context.log.info("Registering new model version.")
        model_ref = registry.log_model(
            model,
            model_name=model_name,
            comment="Toy model used to predict the margin of transactions.",
            sample_input_data=df.drop("TRANSACTION_MARGIN"),
            metrics={"score": new_score},
        )
        version_name = model_ref.version_name
        model = registry.get_model(model_name)
        model.default = version_name
    
        return {"version": version_name, "score": new_score}
    
    else:
        context.log.info("New model performance worse than previous version, "
                         "retaining previous version.")
        return {"version": old_version_name, "score": old_score}

def _train_model(df, context: dg.AssetExecutionContext) -> BaseTransformer:
    """Perform a lightweight model selection loop over candidate regressors.

    Args:
        df: Training dataset prepared from Snowflake tables.
        context: Dagster execution context used for logging model selection progress.

    Returns:
        snowflake.ml.modeling.framework.base.BaseTransformer: The highest scoring model
        pipeline fit on the provided training data.
    """
    # toy dataset, propper train test split would be done here
    train = df
    test = df

    # toy model, proper model selection would be done here with a grid search
    # this would also typically use the container service so it could be distributed
    # across a cluster as an async job, rather than on the warehouse sequentially
    # just for demonstration
    selected_model = None
    selected_type = None
    top_score = 0

    for name, transformer in (("xgboost", XGBRegressor), ("linear", LinearRegression)):
        context.log.info(f"training {name} regression model")
        model = Pipeline(steps=[
                ("onehot", OneHotEncoder(
                    categories="auto",
                    input_cols=["SALES_CHANNEL"],
                    output_cols=["SALES_CHANNEL"],
                    drop_input_cols=True
                )),
                ("scale", StandardScaler(
                    input_cols=["TRANSACTION_REVENUE"],
                    output_cols=["TRANSACTION_REVENUE"]
                )),
                ("reg", transformer(
                    label_cols=["TRANSACTION_MARGIN"],
                    output_cols=["TRANSACTION_MARGIN_PRED"],
                    drop_input_cols=True
                ))
        ]).fit(train)
        score = int(model.score(test)) # type: ignore
        if score > top_score:
            selected_model = model
            selected_type = name
            top_score = score


    context.log.info(f"{selected_type} model selected")
    return selected_model # type: ignore

def _get_train_data(session: Session) -> DataFrame:
    """Fetch and type cast the training dataset from Snowflake.

    Args:
        session: Active Snowpark session used to query Snowflake tables.

    Returns:
        snowflake.snowpark.dataframe.DataFrame: Training dataset with numeric columns
        cast to ``double`` for compatibility with the ML pipeline.
    """
    return (
        session.table("transactions")
        .select(
            "sales_channel",
            F.col("transaction_revenue").cast("double").alias("transaction_revenue"),
            F.col("transaction_margin").cast("double").alias("transaction_margin"),
        )
    )

def _get_validation_data(session: Session) -> DataFrame:
    """Fetch and type cast the validation dataset from Snowflake.

    Args:
        session: Active Snowpark session used to query Snowflake tables.

    Returns:
        snowflake.snowpark.dataframe.DataFrame: Validation dataset mirroring the
        training schema for evaluation purposes.
    """
    return (
        session.table("transactions")
        .select(
            "sales_channel",
            F.col("transaction_revenue").cast("double").alias("transaction_revenue"),
            F.col("transaction_margin").cast("double").alias("transaction_margin"),
        )
    )
