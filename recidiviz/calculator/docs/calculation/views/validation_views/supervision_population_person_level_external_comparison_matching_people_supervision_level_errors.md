## validation_views.supervision_population_person_level_external_comparison_matching_people_supervision_level_errors

Compares internal and external lists of person-level supervision populations among rows where we both agree the person is on supervision.
 Only includes external sources with supervision level information.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=supervision_population_person_level_external_comparison_matching_people_supervision_level_errors)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=supervision_population_person_level_external_comparison_matching_people_supervision_level_errors)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.supervision_population_person_level_external_comparison_matching_people_supervision_level_errors](../validation_views/supervision_population_person_level_external_comparison_matching_people_supervision_level_errors.md) <br/>
|--validation_external_accuracy_tables.supervision_population_person_level ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_external_accuracy_tables&t=supervision_population_person_level)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_external_accuracy_tables&t=supervision_population_person_level)) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
