## validation_views.invalid_pfi_for_temporary_custody_admissions
Incarceration admission metrics with invalid pfi values for TEMPORARY_CUSTODY admissions.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=invalid_pfi_for_temporary_custody_admissions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=invalid_pfi_for_temporary_custody_admissions)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.invalid_pfi_for_temporary_custody_admissions](../validation_views/invalid_pfi_for_temporary_custody_admissions.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_not_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
