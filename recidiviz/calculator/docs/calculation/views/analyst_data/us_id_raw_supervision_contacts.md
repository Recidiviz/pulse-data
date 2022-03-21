## analyst_data.us_id_raw_supervision_contacts
View pulling from raw data in US_ID that captures supervision contacts, not limited to actual contacts with a client

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=analyst_data&t=us_id_raw_supervision_contacts)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=analyst_data&t=us_id_raw_supervision_contacts)
<br/>

#### Dependency Trees

##### Parentage
[analyst_data.us_id_raw_supervision_contacts](../analyst_data/us_id_raw_supervision_contacts.md) <br/>
|--us_id_raw_data_up_to_date_views.sprvsn_cntc_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/sprvsn_cntc.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=sprvsn_cntc_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=sprvsn_cntc_latest)) <br/>
|--us_id_raw_data_up_to_date_views.cntc_typ_cd_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/cntc_typ_cd.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=cntc_typ_cd_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=cntc_typ_cd_latest)) <br/>
|--us_id_raw_data_up_to_date_views.cntc_title_cd_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/cntc_title_cd.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=cntc_title_cd_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=cntc_title_cd_latest)) <br/>
|--us_id_raw_data_up_to_date_views.cntc_rslt_cd_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/cntc_rslt_cd.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=cntc_rslt_cd_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=cntc_rslt_cd_latest)) <br/>
|--us_id_raw_data_up_to_date_views.cntc_loc_cd_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/cntc_loc_cd.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=cntc_loc_cd_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=cntc_loc_cd_latest)) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>


##### Descendants
[analyst_data.us_id_raw_supervision_contacts](../analyst_data/us_id_raw_supervision_contacts.md) <br/>
|--[analyst_data.us_id_behavior_responses](../analyst_data/us_id_behavior_responses.md) <br/>
|----[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|------[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>

