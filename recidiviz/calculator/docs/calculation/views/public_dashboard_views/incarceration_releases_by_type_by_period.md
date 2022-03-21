## public_dashboard_views.incarceration_releases_by_type_by_period
Incarceration releases by period broken down by release type and demographics.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=incarceration_releases_by_type_by_period)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=incarceration_releases_by_type_by_period)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.incarceration_releases_by_type_by_period](../public_dashboard_views/incarceration_releases_by_type_by_period.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>


##### Descendants
[public_dashboard_views.incarceration_releases_by_type_by_period](../public_dashboard_views/incarceration_releases_by_type_by_period.md) <br/>
|--[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|--[validation_views.incarceration_releases_by_type_by_period_internal_consistency](../validation_views/incarceration_releases_by_type_by_period_internal_consistency.md) <br/>
|--[validation_views.incarceration_releases_by_type_by_period_internal_consistency_errors](../validation_views/incarceration_releases_by_type_by_period_internal_consistency_errors.md) <br/>

