## validation_views.admission_pfi_pop_pfi_mismatch

A view revealing when the purpose_for_incarceration value on an admission metric
does not match the purpose_for_incarceration value on a population metric for the same
person_id and metric date.

Mismatches here indicate that IP pre-processing is not handling zero-day incarceration 
periods with different PFI values than the PFI on another period that starts on the same
day but extends to at least the next day.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=admission_pfi_pop_pfi_mismatch)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=admission_pfi_pop_pfi_mismatch)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.admission_pfi_pop_pfi_mismatch](../validation_views/admission_pfi_pop_pfi_mismatch.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_not_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_not_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
