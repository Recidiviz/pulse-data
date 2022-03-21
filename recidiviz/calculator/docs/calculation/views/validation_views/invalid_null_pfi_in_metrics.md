## validation_views.invalid_null_pfi_in_metrics
Metrics with invalid null values for purpose_for_incarceration.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=invalid_null_pfi_in_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=invalid_null_pfi_in_metrics)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.invalid_null_pfi_in_metrics](../validation_views/invalid_null_pfi_in_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_not_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
