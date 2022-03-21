## validation_views.incarceration_lengths_by_demographics_internal_consistency
 Builds validation table to ensure
internal consistency across breakdowns in the incarceration_lengths_by_demographics view.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=incarceration_lengths_by_demographics_internal_consistency)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=incarceration_lengths_by_demographics_internal_consistency)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.incarceration_lengths_by_demographics_internal_consistency](../validation_views/incarceration_lengths_by_demographics_internal_consistency.md) <br/>
|--[public_dashboard_views.incarceration_lengths_by_demographics](../public_dashboard_views/incarceration_lengths_by_demographics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
