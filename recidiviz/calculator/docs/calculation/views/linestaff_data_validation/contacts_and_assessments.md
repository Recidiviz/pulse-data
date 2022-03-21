## linestaff_data_validation.contacts_and_assessments



#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=linestaff_data_validation&t=contacts_and_assessments)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=linestaff_data_validation&t=contacts_and_assessments)
<br/>

#### Dependency Trees

##### Parentage
[linestaff_data_validation.contacts_and_assessments](../linestaff_data_validation/contacts_and_assessments.md) <br/>
|--state.state_assessment ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_assessment)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_assessment)) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_case_compliance_metrics](../dataflow_metrics_materialized/most_recent_supervision_case_compliance_metrics.md) <br/>
|----[dataflow_metrics.supervision_case_compliance_metrics](../../metrics/supervision/supervision_case_compliance_metrics.md) <br/>


##### Descendants
[linestaff_data_validation.contacts_and_assessments](../linestaff_data_validation/contacts_and_assessments.md) <br/>
|--[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>

