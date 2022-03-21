## public_dashboard_views.supervision_success_by_period_by_demographics
Rates of successful completion of supervision by period, grouped by demographic dimensions.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_success_by_period_by_demographics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_success_by_period_by_demographics)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_success_by_period_by_demographics](../public_dashboard_views/supervision_success_by_period_by_demographics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_success_metrics](../dataflow_metrics_materialized/most_recent_supervision_success_metrics.md) <br/>
|----[dataflow_metrics.supervision_success_metrics](../../metrics/supervision/supervision_success_metrics.md) <br/>


##### Descendants
[public_dashboard_views.supervision_success_by_period_by_demographics](../public_dashboard_views/supervision_success_by_period_by_demographics.md) <br/>
|--[validation_views.supervision_success_by_period_by_demographics_internal_consistency](../validation_views/supervision_success_by_period_by_demographics_internal_consistency.md) <br/>
|--[validation_views.supervision_success_by_period_by_demographics_internal_consistency_errors](../validation_views/supervision_success_by_period_by_demographics_internal_consistency_errors.md) <br/>
|--[validation_views.supervision_success_by_period_dashboard_comparison](../validation_views/supervision_success_by_period_dashboard_comparison.md) <br/>
|--[validation_views.supervision_success_by_period_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_period_dashboard_comparisoncompletion_errors.md) <br/>
|--[validation_views.supervision_success_by_period_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_period_dashboard_comparisontermination_errors.md) <br/>

