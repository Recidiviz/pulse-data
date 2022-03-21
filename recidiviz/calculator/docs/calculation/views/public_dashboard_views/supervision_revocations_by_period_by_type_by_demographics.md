## public_dashboard_views.supervision_revocations_by_period_by_type_by_demographics
Supervision revocations by period, by source violation type, and by demographic breakdowns. Person-based counts
    with respect to metric_period_months and supervision_type. If a person has more than one revocation of the same
    supervision type in a given metric period, the most recent one is chosen.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_revocations_by_period_by_type_by_demographics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_revocations_by_period_by_type_by_demographics)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_revocations_by_period_by_type_by_demographics](../public_dashboard_views/supervision_revocations_by_period_by_type_by_demographics.md) <br/>
|--[shared_metric_views.event_based_commitments_from_supervision](../shared_metric_views/event_based_commitments_from_supervision.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>


##### Descendants
[public_dashboard_views.supervision_revocations_by_period_by_type_by_demographics](../public_dashboard_views/supervision_revocations_by_period_by_type_by_demographics.md) <br/>
|--[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|--[validation_views.revocations_by_period_dashboard_comparison](../validation_views/revocations_by_period_dashboard_comparison.md) <br/>
|--[validation_views.revocations_by_period_dashboard_comparison_errors](../validation_views/revocations_by_period_dashboard_comparison_errors.md) <br/>
|--[validation_views.supervision_revocations_by_period_by_type_by_demographics_internal_consistency](../validation_views/supervision_revocations_by_period_by_type_by_demographics_internal_consistency.md) <br/>
|--[validation_views.supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors](../validation_views/supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors.md) <br/>

