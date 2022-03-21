## public_dashboard_views.supervision_population_by_district_by_demographics
Most recent daily supervision population counts broken down by district and demographic categories.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=supervision_population_by_district_by_demographics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=supervision_population_by_district_by_demographics)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.supervision_population_by_district_by_demographics](../public_dashboard_views/supervision_population_by_district_by_demographics.md) <br/>
|--[shared_metric_views.single_day_supervision_population_for_spotlight](../shared_metric_views/single_day_supervision_population_for_spotlight.md) <br/>
|----[dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_single_day_supervision_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
[public_dashboard_views.supervision_population_by_district_by_demographics](../public_dashboard_views/supervision_population_by_district_by_demographics.md) <br/>
|--[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|--[validation_views.supervision_population_by_district_by_demographics_internal_consistency](../validation_views/supervision_population_by_district_by_demographics_internal_consistency.md) <br/>
|--[validation_views.supervision_population_by_district_by_demographics_internal_consistency_errors](../validation_views/supervision_population_by_district_by_demographics_internal_consistency_errors.md) <br/>

