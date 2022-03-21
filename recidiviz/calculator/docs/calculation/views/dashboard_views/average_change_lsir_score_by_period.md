## dashboard_views.average_change_lsir_score_by_period

The average change in LSIR score by metric period months of scheduled 
supervision termination. Per ND-request, compares the LSIR score at 
termination to the second LSIR score of the person's supervision.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=average_change_lsir_score_by_period)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=average_change_lsir_score_by_period)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.average_change_lsir_score_by_period](../dashboard_views/average_change_lsir_score_by_period.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|----[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
