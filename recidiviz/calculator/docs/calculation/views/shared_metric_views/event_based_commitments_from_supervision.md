## shared_metric_views.event_based_commitments_from_supervision

 Commitment from supervision admission data on the person level with violation and 
 admission information.

 Expanded Dimensions: district, supervision_type
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=shared_metric_views&t=event_based_commitments_from_supervision)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=shared_metric_views&t=event_based_commitments_from_supervision)
<br/>

#### Dependency Trees

##### Parentage
[shared_metric_views.event_based_commitments_from_supervision](../shared_metric_views/event_based_commitments_from_supervision.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>


##### Descendants
[shared_metric_views.event_based_commitments_from_supervision](../shared_metric_views/event_based_commitments_from_supervision.md) <br/>
|--[dashboard_views.admissions_by_type_by_month](../dashboard_views/admissions_by_type_by_month.md) <br/>
|--[dashboard_views.admissions_by_type_by_period](../dashboard_views/admissions_by_type_by_period.md) <br/>
|--[dashboard_views.revocations_by_month](../dashboard_views/revocations_by_month.md) <br/>
|--[dashboard_views.revocations_by_officer_by_period](../dashboard_views/revocations_by_officer_by_period.md) <br/>
|--[dashboard_views.revocations_by_period](../dashboard_views/revocations_by_period.md) <br/>
|----[validation_views.revocations_by_period_dashboard_comparison](../validation_views/revocations_by_period_dashboard_comparison.md) <br/>
|----[validation_views.revocations_by_period_dashboard_comparison_errors](../validation_views/revocations_by_period_dashboard_comparison_errors.md) <br/>
|--[dashboard_views.revocations_by_race_and_ethnicity_by_period](../dashboard_views/revocations_by_race_and_ethnicity_by_period.md) <br/>
|--[dashboard_views.revocations_by_site_id_by_period](../dashboard_views/revocations_by_site_id_by_period.md) <br/>
|--[dashboard_views.revocations_by_supervision_type_by_month](../dashboard_views/revocations_by_supervision_type_by_month.md) <br/>
|--[dashboard_views.revocations_by_violation_type_by_month](../dashboard_views/revocations_by_violation_type_by_month.md) <br/>
|--[public_dashboard_views.supervision_revocations_by_period_by_type_by_demographics](../public_dashboard_views/supervision_revocations_by_period_by_type_by_demographics.md) <br/>
|----[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|----[validation_views.revocations_by_period_dashboard_comparison](../validation_views/revocations_by_period_dashboard_comparison.md) <br/>
|----[validation_views.revocations_by_period_dashboard_comparison_errors](../validation_views/revocations_by_period_dashboard_comparison_errors.md) <br/>
|----[validation_views.supervision_revocations_by_period_by_type_by_demographics_internal_consistency](../validation_views/supervision_revocations_by_period_by_type_by_demographics_internal_consistency.md) <br/>
|----[validation_views.supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors](../validation_views/supervision_revocations_by_period_by_type_by_demographics_internal_consistency_errors.md) <br/>

