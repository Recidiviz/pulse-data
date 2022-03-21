## public_dashboard_views.supervision_terminations_by_month
Rates of successful supervision terminations by month.

This is typically used as a backup for the corresponding supervision_success* view when that view produces unreliable data (e.g. when the sentencing data in a state is inaccurate).

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_terminations_by_month)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_terminations_by_month)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_terminations_by_month](../public_dashboard_views/supervision_terminations_by_month.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|----[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
