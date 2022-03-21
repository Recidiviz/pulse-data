## validation_views.incarceration_admission_external_prod_staging_comparison

Comparison of external, prod, and staging data on incarceration admissions


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=incarceration_admission_external_prod_staging_comparison)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=incarceration_admission_external_prod_staging_comparison)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.incarceration_admission_external_prod_staging_comparison](../validation_views/incarceration_admission_external_prod_staging_comparison.md) <br/>
|--[validation_views.incarceration_admission_person_level_external_comparison](../validation_views/incarceration_admission_person_level_external_comparison.md) <br/>
|----validation_external_accuracy_tables.incarceration_admission_person_level ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_external_accuracy_tables&t=incarceration_admission_person_level)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_external_accuracy_tables&t=incarceration_admission_person_level)) <br/>
|----state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
