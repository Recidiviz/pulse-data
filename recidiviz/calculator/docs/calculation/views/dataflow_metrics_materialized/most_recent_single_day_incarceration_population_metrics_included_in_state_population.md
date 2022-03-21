## dataflow_metrics_materialized.most_recent_single_day_incarceration_population_metrics_included_in_state_population
incarceration_population_metrics output for the most recent single day recorded for this metric

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_single_day_incarceration_population_metrics_included_in_state_population)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_single_day_incarceration_population_metrics_included_in_state_population)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_single_day_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_single_day_incarceration_population_metrics_included_in_state_population.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_single_day_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_single_day_incarceration_population_metrics_included_in_state_population.md) <br/>
|--[dashboard_views.prison_population_snapshot_by_dimension](../dashboard_views/prison_population_snapshot_by_dimension.md) <br/>
|--[dashboard_views.prison_population_snapshot_person_level](../dashboard_views/prison_population_snapshot_person_level.md) <br/>
|--[shared_metric_views.single_day_incarceration_population_for_spotlight](../shared_metric_views/single_day_incarceration_population_for_spotlight.md) <br/>
|----[public_dashboard_views.community_corrections_population_by_facility_by_demographics](../public_dashboard_views/community_corrections_population_by_facility_by_demographics.md) <br/>
|----[public_dashboard_views.incarceration_population_by_admission_reason](../public_dashboard_views/incarceration_population_by_admission_reason.md) <br/>
|------[validation_views.incarceration_population_by_admission_reason_internal_consistency](../validation_views/incarceration_population_by_admission_reason_internal_consistency.md) <br/>
|------[validation_views.incarceration_population_by_admission_reason_internal_consistency_errors](../validation_views/incarceration_population_by_admission_reason_internal_consistency_errors.md) <br/>
|------[validation_views.incarceration_population_by_demographic_internal_comparison](../validation_views/incarceration_population_by_demographic_internal_comparison.md) <br/>
|------[validation_views.incarceration_population_by_demographic_internal_comparison_errors](../validation_views/incarceration_population_by_demographic_internal_comparison_errors.md) <br/>
|----[public_dashboard_views.incarceration_population_by_facility_by_demographics](../public_dashboard_views/incarceration_population_by_facility_by_demographics.md) <br/>
|------[validation_views.incarceration_population_by_demographic_internal_comparison](../validation_views/incarceration_population_by_demographic_internal_comparison.md) <br/>
|------[validation_views.incarceration_population_by_demographic_internal_comparison_errors](../validation_views/incarceration_population_by_demographic_internal_comparison_errors.md) <br/>
|------[validation_views.incarceration_population_by_facility_by_demographics_internal_consistency](../validation_views/incarceration_population_by_facility_by_demographics_internal_consistency.md) <br/>
|------[validation_views.incarceration_population_by_facility_by_demographics_internal_consistency_errors](../validation_views/incarceration_population_by_facility_by_demographics_internal_consistency_errors.md) <br/>
|----[public_dashboard_views.sentence_type_by_district_by_demographics](../public_dashboard_views/sentence_type_by_district_by_demographics.md) <br/>
|------[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|------[validation_views.sentence_type_by_district_by_demographics_internal_consistency](../validation_views/sentence_type_by_district_by_demographics_internal_consistency.md) <br/>
|------[validation_views.sentence_type_by_district_by_demographics_internal_consistency_errors](../validation_views/sentence_type_by_district_by_demographics_internal_consistency_errors.md) <br/>

