## validation_views.supervision_termination_person_level_external_comparison_errors

Comparison of internal and external lists of supervision terminations.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=supervision_termination_person_level_external_comparison_errors)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=supervision_termination_person_level_external_comparison_errors)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.supervision_termination_person_level_external_comparison_errors](../validation_views/supervision_termination_person_level_external_comparison_errors.md) <br/>
|--validation_external_accuracy_tables.supervision_termination_person_level ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_external_accuracy_tables&t=supervision_termination_person_level)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_external_accuracy_tables&t=supervision_termination_person_level)) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|----[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
