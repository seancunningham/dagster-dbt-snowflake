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

from .....utils.automation_conditions import CustomAutomationCondition
from ...resources import SnowparkResource

MODEL_NAME = "margin_prediction"
RETRAIN_THRESHOLD = 0.7
DESCRIPTION = """Toy model used to predict the margin of transactions."""

@dg.asset(
        key=["ml", "model", "margin_prediction"],
        deps=[["transaction_db", "stg", "transactions"]],
        kinds={"snowpark", "xgboost"},
        group_name="data_science",
        description=DESCRIPTION,
        automation_condition=CustomAutomationCondition.on_cron("@daily")
)
def margin_prediction(
        context: dg.AssetExecutionContext,
        snowpark: SnowparkResource) -> dg.MaterializeResult:

    session = snowpark.get_session(schema="transaction_db")

    val = get_validation_data(session)
    registry = Registry(session)
    try:
        model_ref = registry.get_model(MODEL_NAME).default
        
    except Exception:
        model_ref = None
    
    if model_ref:
        context.log.info("previous version found, checking score.")
        model = model_ref.load() 
        old_version_name = model_ref.version_name
        old_score = model.score(val)
        model_ref.set_metric("score", old_score)
        if old_score >= RETRAIN_THRESHOLD:
            context.log.info("Score above threshold, skipping retrain.")
            return dg.MaterializeResult(
                metadata={
                    "version": dg.TextMetadataValue(model_ref.version_name),
                    "score": dg.FloatMetadataValue(old_score)
                }
            )
        else:
            context.log.info("Score below threshold, starting retrain.")

    else:
        old_score = 0
        old_version_name = "not_registered"

    context.log.info("Training model.")
    df = get_train_data(session)
    model = train_model(df, context)
    new_score = float(model.score(val)) # type: ignore

    if new_score > old_score:
        context.log.info("Registering new model version.")
        model_ref = registry.log_model(
            model,
            model_name=MODEL_NAME,
            comment=DESCRIPTION,
            sample_input_data=df.drop("TRANSACTION_MARGIN"),
            metrics={"score": new_score},
        )
        version_name = model_ref.version_name
        model = registry.get_model(MODEL_NAME)
        model.default = version_name
    
        return dg.MaterializeResult(
            metadata={
                "version": dg.TextMetadataValue(version_name),
                "score": dg.FloatMetadataValue(new_score)
            }
        )
    
    else:
        context.log.info("New model performance worse than previous version, "
                         "retaining previous version.")
        return dg.MaterializeResult(
            metadata={
                "version": dg.TextMetadataValue(old_version_name),
                "score": dg.FloatMetadataValue(old_score)
            }
        )
        


@dg.asset_check(
        asset=["ml", "model", "margin_prediction"],
        description=("Check that default version of model scores above"
                     "threshold for retraining")
)
def score_above_threshold(snowpark: SnowparkResource) -> dg.AssetCheckResult:
    session = snowpark.get_session(schema="transaction_db")
    registry = Registry(session)
    model = registry.get_model(MODEL_NAME).default
    score = model.get_metric("score")
    return dg.AssetCheckResult(
        passed=bool(score > RETRAIN_THRESHOLD)
    )

def train_model(df, context: dg.AssetExecutionContext) -> BaseTransformer:
    # toy dataset, propper train test split would be done here
    train = df
    test = df

    preprocessor = (
            ("onehot", OneHotEncoder(
                categories="auto",
                input_cols=["SALES_CHANNEL"],
                output_cols=["SALES_CHANNEL"],
                drop_input_cols=True
            )),
            ("scale", StandardScaler(
                input_cols=["TRANSACTION_REVENUE"],
                output_cols=["TRANSACTION_REVENUE"]
            ))
    )

    # toy model, proper model selection would be done here with a grid search
    # this would also typically use the container service so it could be distributed
    # across a cluster as an async job, rather than on the warehouse sequentially
    # just for demonstration
    context.log.info("training xgboost model")
    xgb_regression_model = Pipeline(steps=[
                ("prep", preprocessor),
                ("reg", XGBRegressor(
                    label_cols=["TRANSACTION_MARGIN"],
                    output_cols=["TRANSACTION_MARGIN_PRED"],
                    drop_input_cols=True
                ))
    ]).fit(train)

    context.log.info("training linear regression model")
    linear_regression_model = Pipeline(steps=[
                ("prep", preprocessor),
                ("reg", LinearRegression(
                    label_cols=["TRANSACTION_MARGIN"],
                    output_cols=["TRANSACTION_MARGIN_PRED"],
                    drop_input_cols=True
                ))
    ]).fit(train)

    xgb_regression_score = int(xgb_regression_model.score(test)) # type: ignore
    linear_regression_score = int(linear_regression_model.score(test))  # type: ignore

    if xgb_regression_score >= linear_regression_score:
        context.log.info("xgboost model selected")
        return xgb_regression_model
    else:
        context.log.info("linear regression model selected")
        return linear_regression_model

def get_train_data(session: Session) -> DataFrame:
    return (
        session.table("transactions")
        .select(
            "sales_channel",
            F.col("transaction_revenue").cast("double").alias("transaction_revenue"),
            F.col("transaction_margin").cast("double").alias("transaction_margin"),
        )
    )

def get_test_data(session: Session) -> DataFrame:
    return get_train_data(session)

def get_validation_data(session) -> DataFrame:
    return get_train_data(session)