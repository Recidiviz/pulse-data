## validation_views.supervision_start_person_level_external_comparison

Comparison of internal and external lists of supervision starts.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=supervision_start_person_level_external_comparison)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=supervision_start_person_level_external_comparison)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.supervision_start_person_level_external_comparison](../validation_views/supervision_start_person_level_external_comparison.md) <br/>
|--validation_external_accuracy_tables.supervision_start_person_level ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_external_accuracy_tables&t=supervision_start_person_level)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_external_accuracy_tables&t=supervision_start_person_level)) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_start_metrics](../dataflow_metrics_materialized/most_recent_supervision_start_metrics.md) <br/>
|----[dataflow_metrics.supervision_start_metrics](../../metrics/supervision/supervision_start_metrics.md) <br/>


##### Descendants
[validation_views.supervision_start_person_level_external_comparison](../validation_views/supervision_start_person_level_external_comparison.md) <br/>
|--[validation_views.supervision_start_external_prod_staging_comparison](../validation_views/supervision_start_external_prod_staging_comparison.md) <br/>

