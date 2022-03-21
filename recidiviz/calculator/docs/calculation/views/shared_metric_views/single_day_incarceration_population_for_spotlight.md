## shared_metric_views.single_day_incarceration_population_for_spotlight
Event based incarceration population for the most recent date of incarceration.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=shared_metric_views&t=single_day_incarceration_population_for_spotlight)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=shared_metric_views&t=single_day_incarceration_population_for_spotlight)
<br/>

#### Dependency Trees

##### Parentage
[shared_metric_views.single_day_incarceration_population_for_spotlight](../shared_metric_views/single_day_incarceration_population_for_spotlight.md) <br/>
|--[dataflow_metrics_materialized.most_recent_single_day_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_single_day_incarceration_population_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>


##### Descendants
[shared_metric_views.single_day_incarceration_population_for_spotlight](../shared_metric_views/single_day_incarceration_population_for_spotlight.md) <br/>
|--[public_dashboard_views.community_corrections_population_by_facility_by_demographics](../public_dashboard_views/community_corrections_population_by_facility_by_demographics.md) <br/>
|--[public_dashboard_views.incarceration_population_by_admission_reason](../public_dashboard_views/incarceration_population_by_admission_reason.md) <br/>
|----[validation_views.incarceration_population_by_admission_reason_internal_consistency](../validation_views/incarceration_population_by_admission_reason_internal_consistency.md) <br/>
|----[validation_views.incarceration_population_by_admission_reason_internal_consistency_errors](../validation_views/incarceration_population_by_admission_reason_internal_consistency_errors.md) <br/>
|----[validation_views.incarceration_population_by_demographic_internal_comparison](../validation_views/incarceration_population_by_demographic_internal_comparison.md) <br/>
|----[validation_views.incarceration_population_by_demographic_internal_comparison_errors](../validation_views/incarceration_population_by_demographic_internal_comparison_errors.md) <br/>
|--[public_dashboard_views.incarceration_population_by_facility_by_demographics](../public_dashboard_views/incarceration_population_by_facility_by_demographics.md) <br/>
|----[validation_views.incarceration_population_by_demographic_internal_comparison](../validation_views/incarceration_population_by_demographic_internal_comparison.md) <br/>
|----[validation_views.incarceration_population_by_demographic_internal_comparison_errors](../validation_views/incarceration_population_by_demographic_internal_comparison_errors.md) <br/>
|----[validation_views.incarceration_population_by_facility_by_demographics_internal_consistency](../validation_views/incarceration_population_by_facility_by_demographics_internal_consistency.md) <br/>
|----[validation_views.incarceration_population_by_facility_by_demographics_internal_consistency_errors](../validation_views/incarceration_population_by_facility_by_demographics_internal_consistency_errors.md) <br/>
|--[public_dashboard_views.sentence_type_by_district_by_demographics](../public_dashboard_views/sentence_type_by_district_by_demographics.md) <br/>
|----[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|----[validation_views.sentence_type_by_district_by_demographics_internal_consistency](../validation_views/sentence_type_by_district_by_demographics_internal_consistency.md) <br/>
|----[validation_views.sentence_type_by_district_by_demographics_internal_consistency_errors](../validation_views/sentence_type_by_district_by_demographics_internal_consistency_errors.md) <br/>

