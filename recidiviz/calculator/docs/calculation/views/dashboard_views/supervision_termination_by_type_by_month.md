## dashboard_views.supervision_termination_by_type_by_month

 Supervision termination by type and by month.
 The counts of supervision that were projected to end in a given month and
 that have ended by now, broken down by whether or not the
 supervision ended because of a revocation or successful completion.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=supervision_termination_by_type_by_month)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=supervision_termination_by_type_by_month)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.supervision_termination_by_type_by_month](../dashboard_views/supervision_termination_by_type_by_month.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_success_metrics](../dataflow_metrics_materialized/most_recent_supervision_success_metrics.md) <br/>
|----[dataflow_metrics.supervision_success_metrics](../../metrics/supervision/supervision_success_metrics.md) <br/>


##### Descendants
[dashboard_views.supervision_termination_by_type_by_month](../dashboard_views/supervision_termination_by_type_by_month.md) <br/>
|--[validation_views.supervision_success_by_month_dashboard_comparison](../validation_views/supervision_success_by_month_dashboard_comparison.md) <br/>
|--[validation_views.supervision_success_by_month_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_month_dashboard_comparisoncompletion_errors.md) <br/>
|--[validation_views.supervision_success_by_month_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_month_dashboard_comparisontermination_errors.md) <br/>

