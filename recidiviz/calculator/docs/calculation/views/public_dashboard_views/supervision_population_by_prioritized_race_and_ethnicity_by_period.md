## public_dashboard_views.supervision_population_by_prioritized_race_and_ethnicity_by_period
Supervision population by metric period, broken down by race/ethnicity, where a person is counted towards only
    the race/ethnicity category that is least represented in the population of the state.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_population_by_prioritized_race_and_ethnicity_by_period)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_population_by_prioritized_race_and_ethnicity_by_period)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_population_by_prioritized_race_and_ethnicity_by_period](../public_dashboard_views/supervision_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
|--[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
[public_dashboard_views.supervision_population_by_prioritized_race_and_ethnicity_by_period](../public_dashboard_views/supervision_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
|--[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|--[validation_views.supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency](../validation_views/supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency.md) <br/>
|--[validation_views.supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency_errors](../validation_views/supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency_errors.md) <br/>

