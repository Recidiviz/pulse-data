## validation_views.supervision_success_by_period_by_demographics_internal_consistency
 Builds validation table to ensure
internal consistency across breakdowns in the supervision_success_by_period_by_demographics view.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=supervision_success_by_period_by_demographics_internal_consistency)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=supervision_success_by_period_by_demographics_internal_consistency)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.supervision_success_by_period_by_demographics_internal_consistency](../validation_views/supervision_success_by_period_by_demographics_internal_consistency.md) <br/>
|--[public_dashboard_views.supervision_success_by_period_by_demographics](../public_dashboard_views/supervision_success_by_period_by_demographics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_success_metrics](../dataflow_metrics_materialized/most_recent_supervision_success_metrics.md) <br/>
|------[dataflow_metrics.supervision_success_metrics](../../metrics/supervision/supervision_success_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
