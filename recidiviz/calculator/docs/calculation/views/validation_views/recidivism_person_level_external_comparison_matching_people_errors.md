## validation_views.recidivism_person_level_external_comparison_matching_people_errors

Comparison of recidivism values between internal and external lists of person-level recidivism among rows where we both
agree the person was included in a release cohort.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=recidivism_person_level_external_comparison_matching_people_errors)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=recidivism_person_level_external_comparison_matching_people_errors)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.recidivism_person_level_external_comparison_matching_people_errors](../validation_views/recidivism_person_level_external_comparison_matching_people_errors.md) <br/>
|--validation_external_accuracy_tables.recidivism_person_level ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_external_accuracy_tables&t=recidivism_person_level)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_external_accuracy_tables&t=recidivism_person_level)) <br/>
|--[dataflow_metrics_materialized.most_recent_recidivism_rate_metrics](../dataflow_metrics_materialized/most_recent_recidivism_rate_metrics.md) <br/>
|----[dataflow_metrics.recidivism_rate_metrics](../../metrics/recidivism/recidivism_rate_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
