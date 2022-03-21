## public_dashboard_views.supervision_success_by_month
Rates of successful supervision completion by month.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_success_by_month)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_success_by_month)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_success_by_month](../public_dashboard_views/supervision_success_by_month.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_success_metrics](../dataflow_metrics_materialized/most_recent_supervision_success_metrics.md) <br/>
|----[dataflow_metrics.supervision_success_metrics](../../metrics/supervision/supervision_success_metrics.md) <br/>


##### Descendants
[public_dashboard_views.supervision_success_by_month](../public_dashboard_views/supervision_success_by_month.md) <br/>
|--[validation_views.supervision_success_by_month_dashboard_comparison](../validation_views/supervision_success_by_month_dashboard_comparison.md) <br/>
|--[validation_views.supervision_success_by_month_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_month_dashboard_comparisoncompletion_errors.md) <br/>
|--[validation_views.supervision_success_by_month_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_month_dashboard_comparisontermination_errors.md) <br/>

