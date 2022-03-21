## reference_views.us_id_case_update_info
Provides agent case update notes for people on supervision in US_ID
    

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=reference_views&t=us_id_case_update_info)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=reference_views&t=us_id_case_update_info)
<br/>

#### Dependency Trees

##### Parentage
[reference_views.us_id_case_update_info](../reference_views/us_id_case_update_info.md) <br/>
|--us_id_raw_data_up_to_date_views.agnt_case_updt_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/agnt_case_updt.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=agnt_case_updt_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=agnt_case_updt_latest)) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>


##### Descendants
This view has no child dependencies.
