## dataflow_metrics_materialized.most_recent_incarceration_release_metrics_not_included_in_state_population
incarceration_release_metrics for the most recent job run, for output that is not included in the state's population.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_incarceration_release_metrics_not_included_in_state_population)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_incarceration_release_metrics_not_included_in_state_population)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_not_included_in_state_population.md) <br/>
|--[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_not_included_in_state_population.md) <br/>
|--[validation_views.invalid_admitted_from_supervision_admission_reason](../validation_views/invalid_admitted_from_supervision_admission_reason.md) <br/>
|--[validation_views.invalid_null_pfi_in_metrics](../validation_views/invalid_null_pfi_in_metrics.md) <br/>
|--[validation_views.invalid_release_reasons_for_temporary_custody](../validation_views/invalid_release_reasons_for_temporary_custody.md) <br/>

