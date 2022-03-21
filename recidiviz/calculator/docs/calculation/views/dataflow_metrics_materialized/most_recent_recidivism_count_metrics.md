## dataflow_metrics_materialized.most_recent_recidivism_count_metrics
recidivism_count_metrics for the most recent job run

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_recidivism_count_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_recidivism_count_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_recidivism_count_metrics](../dataflow_metrics_materialized/most_recent_recidivism_count_metrics.md) <br/>
|--[dataflow_metrics.recidivism_count_metrics](../../metrics/recidivism/recidivism_count_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_recidivism_count_metrics](../dataflow_metrics_materialized/most_recent_recidivism_count_metrics.md) <br/>
|--[dashboard_views.avg_days_at_liberty_by_month](../dashboard_views/avg_days_at_liberty_by_month.md) <br/>
|--[dashboard_views.reincarcerations_by_month](../dashboard_views/reincarcerations_by_month.md) <br/>
|--[dashboard_views.reincarcerations_by_period](../dashboard_views/reincarcerations_by_period.md) <br/>
|--[sessions.reincarceration_sessions_from_dataflow](../sessions/reincarceration_sessions_from_dataflow.md) <br/>
|----[validation_views.reincarcerations_from_dataflow_to_dataflow_disaggregated](../validation_views/reincarcerations_from_dataflow_to_dataflow_disaggregated.md) <br/>
|--[validation_views.reincarcerations_from_dataflow_to_dataflow_disaggregated](../validation_views/reincarcerations_from_dataflow_to_dataflow_disaggregated.md) <br/>
|--[validation_views.reincarcerations_from_sessions_to_dataflow_disaggregated](../validation_views/reincarcerations_from_sessions_to_dataflow_disaggregated.md) <br/>

