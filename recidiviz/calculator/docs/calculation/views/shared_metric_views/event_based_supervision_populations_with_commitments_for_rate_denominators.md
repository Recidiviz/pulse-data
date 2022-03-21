## shared_metric_views.event_based_supervision_populations_with_commitments_for_rate_denominators

 Event-based supervision by person, where a person is included in the supervision
 population on the day of their commitment from supervision.

 This is necessary whenever we do commitment rate calculations within the supervision
 population. This view ensures that all individuals with commitment admissions are
 included in the supervision populations for rate calculations where the supervision
 population is the denominator and the numerator is some subset of commitments.

 For example, if we want to calculate the percent of an officer's caseload
 that had a treatment sanction commitment admission in a given month, we need to
 ensure that the individuals with the commitment admission are counted as on the
 officer's caseload in that month.

 Expanded Dimensions: district, supervision_type
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=shared_metric_views&t=event_based_supervision_populations_with_commitments_for_rate_denominators)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=shared_metric_views&t=event_based_supervision_populations_with_commitments_for_rate_denominators)
<br/>

#### Dependency Trees

##### Parentage
[shared_metric_views.event_based_supervision_populations_with_commitments_for_rate_denominators](../shared_metric_views/event_based_supervision_populations_with_commitments_for_rate_denominators.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>


##### Descendants
[shared_metric_views.event_based_supervision_populations_with_commitments_for_rate_denominators](../shared_metric_views/event_based_supervision_populations_with_commitments_for_rate_denominators.md) <br/>
|--[dashboard_views.revocations_by_month](../dashboard_views/revocations_by_month.md) <br/>
|--[dashboard_views.revocations_by_officer_by_period](../dashboard_views/revocations_by_officer_by_period.md) <br/>
|--[dashboard_views.revocations_by_period](../dashboard_views/revocations_by_period.md) <br/>
|----[validation_views.revocations_by_period_dashboard_comparison](../validation_views/revocations_by_period_dashboard_comparison.md) <br/>
|----[validation_views.revocations_by_period_dashboard_comparison_errors](../validation_views/revocations_by_period_dashboard_comparison_errors.md) <br/>
|--[dashboard_views.revocations_by_race_and_ethnicity_by_period](../dashboard_views/revocations_by_race_and_ethnicity_by_period.md) <br/>
|--[dashboard_views.revocations_by_site_id_by_period](../dashboard_views/revocations_by_site_id_by_period.md) <br/>
|--[dashboard_views.revocations_by_violation_type_by_month](../dashboard_views/revocations_by_violation_type_by_month.md) <br/>

