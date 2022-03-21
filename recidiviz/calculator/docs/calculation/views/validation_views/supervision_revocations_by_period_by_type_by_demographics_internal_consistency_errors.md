## validation_views.supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors
 Builds validation
table to ensure internal consistency across breakdowns in the
supervision_revocations_by_period_by_type_by_demographics view.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors](../validation_views/supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors.md) <br/>
|--[public_dashboard_views.supervision_revocations_by_period_by_type_by_demographics](../public_dashboard_views/supervision_revocations_by_period_by_type_by_demographics.md) <br/>
|----[shared_metric_views.event_based_commitments_from_supervision](../shared_metric_views/event_based_commitments_from_supervision.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
