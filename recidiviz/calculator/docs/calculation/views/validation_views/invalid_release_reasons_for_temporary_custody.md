## validation_views.invalid_release_reasons_for_temporary_custody
 Incarceration release metrics with invalid release reasons for periods of
    temporary custody or parole board holds.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=invalid_release_reasons_for_temporary_custody)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=invalid_release_reasons_for_temporary_custody)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.invalid_release_reasons_for_temporary_custody](../validation_views/invalid_release_reasons_for_temporary_custody.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_not_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
