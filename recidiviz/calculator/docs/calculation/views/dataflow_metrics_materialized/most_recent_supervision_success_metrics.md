## dataflow_metrics_materialized.most_recent_supervision_success_metrics
supervision_success_metrics for the most recent job run

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_supervision_success_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_supervision_success_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_supervision_success_metrics](../dataflow_metrics_materialized/most_recent_supervision_success_metrics.md) <br/>
|--[dataflow_metrics.supervision_success_metrics](../../metrics/supervision/supervision_success_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_supervision_success_metrics](../dataflow_metrics_materialized/most_recent_supervision_success_metrics.md) <br/>
|--[dashboard_views.supervision_termination_by_type_by_month](../dashboard_views/supervision_termination_by_type_by_month.md) <br/>
|----[validation_views.supervision_success_by_month_dashboard_comparison](../validation_views/supervision_success_by_month_dashboard_comparison.md) <br/>
|----[validation_views.supervision_success_by_month_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_month_dashboard_comparisoncompletion_errors.md) <br/>
|----[validation_views.supervision_success_by_month_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_month_dashboard_comparisontermination_errors.md) <br/>
|--[dashboard_views.supervision_termination_by_type_by_period](../dashboard_views/supervision_termination_by_type_by_period.md) <br/>
|----[validation_views.supervision_success_by_period_dashboard_comparison](../validation_views/supervision_success_by_period_dashboard_comparison.md) <br/>
|----[validation_views.supervision_success_by_period_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_period_dashboard_comparisoncompletion_errors.md) <br/>
|----[validation_views.supervision_success_by_period_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_period_dashboard_comparisontermination_errors.md) <br/>
|--[public_dashboard_views.supervision_success_by_month](../public_dashboard_views/supervision_success_by_month.md) <br/>
|----[validation_views.supervision_success_by_month_dashboard_comparison](../validation_views/supervision_success_by_month_dashboard_comparison.md) <br/>
|----[validation_views.supervision_success_by_month_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_month_dashboard_comparisoncompletion_errors.md) <br/>
|----[validation_views.supervision_success_by_month_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_month_dashboard_comparisontermination_errors.md) <br/>
|--[public_dashboard_views.supervision_success_by_period_by_demographics](../public_dashboard_views/supervision_success_by_period_by_demographics.md) <br/>
|----[validation_views.supervision_success_by_period_by_demographics_internal_consistency](../validation_views/supervision_success_by_period_by_demographics_internal_consistency.md) <br/>
|----[validation_views.supervision_success_by_period_by_demographics_internal_consistency_errors](../validation_views/supervision_success_by_period_by_demographics_internal_consistency_errors.md) <br/>
|----[validation_views.supervision_success_by_period_dashboard_comparison](../validation_views/supervision_success_by_period_dashboard_comparison.md) <br/>
|----[validation_views.supervision_success_by_period_dashboard_comparisoncompletion_errors](../validation_views/supervision_success_by_period_dashboard_comparisoncompletion_errors.md) <br/>
|----[validation_views.supervision_success_by_period_dashboard_comparisontermination_errors](../validation_views/supervision_success_by_period_dashboard_comparisontermination_errors.md) <br/>

