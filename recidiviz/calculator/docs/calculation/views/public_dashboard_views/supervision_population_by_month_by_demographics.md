## public_dashboard_views.supervision_population_by_month_by_demographics
First of the month supervision population counts broken down by demographics.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_population_by_month_by_demographics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_population_by_month_by_demographics)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_population_by_month_by_demographics](../public_dashboard_views/supervision_population_by_month_by_demographics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
