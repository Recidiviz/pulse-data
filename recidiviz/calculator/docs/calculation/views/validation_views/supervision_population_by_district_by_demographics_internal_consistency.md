## validation_views.supervision_population_by_district_by_demographics_internal_consistency
 Builds validation table
to ensure internal consistency across breakdowns in the supervision_population_by_district_by_demographics view.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=supervision_population_by_district_by_demographics_internal_consistency)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=supervision_population_by_district_by_demographics_internal_consistency)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.supervision_population_by_district_by_demographics_internal_consistency](../validation_views/supervision_population_by_district_by_demographics_internal_consistency.md) <br/>
|--[public_dashboard_views.supervision_population_by_district_by_demographics](../public_dashboard_views/supervision_population_by_district_by_demographics.md) <br/>
|----[shared_metric_views.single_day_supervision_population_for_spotlight](../shared_metric_views/single_day_supervision_population_for_spotlight.md) <br/>
|------[dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_single_day_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
