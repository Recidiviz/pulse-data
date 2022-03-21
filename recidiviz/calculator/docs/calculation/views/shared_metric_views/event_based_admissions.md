## shared_metric_views.event_based_admissions

 Admission data on the person level with admission district (county of residence), admission date, and admission reason.

 Expanded Dimensions: district
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=shared_metric_views&t=event_based_admissions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=shared_metric_views&t=event_based_admissions)
<br/>

#### Dependency Trees

##### Parentage
[shared_metric_views.event_based_admissions](../shared_metric_views/event_based_admissions.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>


##### Descendants
[shared_metric_views.event_based_admissions](../shared_metric_views/event_based_admissions.md) <br/>
|--[dashboard_views.admissions_by_type_by_month](../dashboard_views/admissions_by_type_by_month.md) <br/>
|--[dashboard_views.admissions_by_type_by_period](../dashboard_views/admissions_by_type_by_period.md) <br/>
|--[dashboard_views.admissions_versus_releases_by_month](../dashboard_views/admissions_versus_releases_by_month.md) <br/>
|--[dashboard_views.admissions_versus_releases_by_period](../dashboard_views/admissions_versus_releases_by_period.md) <br/>
|--[dashboard_views.reincarcerations_by_month](../dashboard_views/reincarcerations_by_month.md) <br/>
|--[dashboard_views.reincarcerations_by_period](../dashboard_views/reincarcerations_by_period.md) <br/>

