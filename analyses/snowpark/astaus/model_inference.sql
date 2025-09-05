with ml as model _dev_analytics.transaction_db__astaus.margin_prediction
select
    ml!predict(sales_channel, transaction_revenue)['output_feature_0']::number(12,2) pred,
    *
from _dev_analytics.transaction_db__astaus.transactions;