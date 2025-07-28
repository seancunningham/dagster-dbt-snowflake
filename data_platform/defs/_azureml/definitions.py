import dagster as dg

# asset stubs for demonstration, would actually be connected in production

ml_train_marketing_mix_model = dg.AssetSpec(
    key=["ml", "train", "marketing_mix_model"],
    deps=[
        ["adobe_experience", "stg", "hits"],
        ["marketing", "dim", "dim_campaigns"],
    ],
    kinds={"azureml", "mlflow", "pytorch"},
    group_name="data_science"
)


ml_inference_marketing_mix_model = dg.AssetSpec(
    key=["ml", "inference", "marketing_mix_model"],
    deps=[
        ["adobe_experience", "stg", "hits"],
        ["marketing", "dim", "dim_campaigns"],
        ["ml", "train", "marketing_mix_model"],
    ],
    kinds={"azureml", "snowflake"},
    group_name="data_science"
)