## case_triage.etl_officers
etl_officers view with selected columns. 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=case_triage&t=etl_officers)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=case_triage&t=etl_officers)
<br/>

#### Dependency Trees

##### Parentage
[case_triage.etl_officers](../case_triage/etl_officers.md) <br/>
|--static_reference_tables.us_id_roster ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_id_roster)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_id_roster)) <br/>
|--[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|----state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>


##### Descendants
[case_triage.etl_officers](../case_triage/etl_officers.md) <br/>
|--[linestaff_data_validation.metrics_from_po_report](../linestaff_data_validation/metrics_from_po_report.md) <br/>
|--[linestaff_data_validation.po_events](../linestaff_data_validation/po_events.md) <br/>
|--[validation_views.case_triage_etl_freshness](../validation_views/case_triage_etl_freshness.md) <br/>

