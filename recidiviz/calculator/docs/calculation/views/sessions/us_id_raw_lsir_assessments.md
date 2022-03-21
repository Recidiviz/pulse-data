## sessions.us_id_raw_lsir_assessments
Individual questions and components of the LSI-R assessment in ID, derived from raw tables

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sessions&t=us_id_raw_lsir_assessments)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sessions&t=us_id_raw_lsir_assessments)
<br/>

#### Dependency Trees

##### Parentage
[sessions.us_id_raw_lsir_assessments](../sessions/us_id_raw_lsir_assessments.md) <br/>
|--us_id_raw_data_up_to_date_views.tst_qstn_rspns_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/tst_qstn_rspns.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=tst_qstn_rspns_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=tst_qstn_rspns_latest)) <br/>
|--us_id_raw_data_up_to_date_views.ofndr_tst_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/ofndr_tst.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=ofndr_tst_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=ofndr_tst_latest)) <br/>
|--us_id_raw_data_up_to_date_views.assess_qstn_choice_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/assess_qstn_choice.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=assess_qstn_choice_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=assess_qstn_choice_latest)) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|--[sessions.assessment_score_sessions](../sessions/assessment_score_sessions.md) <br/>
|----state.state_assessment ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_assessment)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_assessment)) <br/>
|--[sessions.assessment_lsir_scoring_key](../sessions/assessment_lsir_scoring_key.md) <br/>


##### Descendants
[sessions.us_id_raw_lsir_assessments](../sessions/us_id_raw_lsir_assessments.md) <br/>
|--[sessions.assessment_lsir_responses](../sessions/assessment_lsir_responses.md) <br/>

