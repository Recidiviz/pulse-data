## dataflow_metrics_materialized.most_recent_single_day_program_participation_metrics
program_participation_metrics output for the most recent single day recorded for this metric

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_single_day_program_participation_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_single_day_program_participation_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_single_day_program_participation_metrics](../dataflow_metrics_materialized/most_recent_single_day_program_participation_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_program_participation_metrics](../dataflow_metrics_materialized/most_recent_program_participation_metrics.md) <br/>
|----[dataflow_metrics.program_participation_metrics](../../metrics/program/program_participation_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_single_day_program_participation_metrics](../dataflow_metrics_materialized/most_recent_single_day_program_participation_metrics.md) <br/>
|--[public_dashboard_views.active_program_participation_by_region](../public_dashboard_views/active_program_participation_by_region.md) <br/>
|----[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|----[validation_views.active_program_participation_by_region_internal_consistency](../validation_views/active_program_participation_by_region_internal_consistency.md) <br/>
|----[validation_views.active_program_participation_by_region_internal_consistency_errors](../validation_views/active_program_participation_by_region_internal_consistency_errors.md) <br/>

