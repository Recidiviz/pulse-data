## dataflow_metrics_materialized.most_recent_recidivism_rate_metrics
recidivism_rate_metrics for the most recent job run

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_recidivism_rate_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_recidivism_rate_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_recidivism_rate_metrics](../dataflow_metrics_materialized/most_recent_recidivism_rate_metrics.md) <br/>
|--[dataflow_metrics.recidivism_rate_metrics](../../metrics/recidivism/recidivism_rate_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_recidivism_rate_metrics](../dataflow_metrics_materialized/most_recent_recidivism_rate_metrics.md) <br/>
|--[dashboard_views.reincarceration_rate_by_stay_length](../dashboard_views/reincarceration_rate_by_stay_length.md) <br/>
|--[public_dashboard_views.recidivism_rates_by_cohort_by_year](../public_dashboard_views/recidivism_rates_by_cohort_by_year.md) <br/>
|--[validation_views.recidivism_person_level_external_comparison_matching_people](../validation_views/recidivism_person_level_external_comparison_matching_people.md) <br/>
|--[validation_views.recidivism_person_level_external_comparison_matching_people_errors](../validation_views/recidivism_person_level_external_comparison_matching_people_errors.md) <br/>
|--[validation_views.recidivism_release_cohort_person_level_external_comparison](../validation_views/recidivism_release_cohort_person_level_external_comparison.md) <br/>
|--[validation_views.recidivism_release_cohort_person_level_external_comparison_errors](../validation_views/recidivism_release_cohort_person_level_external_comparison_errors.md) <br/>

