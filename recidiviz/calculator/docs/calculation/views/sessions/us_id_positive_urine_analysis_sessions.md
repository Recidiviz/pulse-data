## sessions.us_id_positive_urine_analysis_sessions
View of periods of time over which the most recent urine analysis test had a positive result

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sessions&t=us_id_positive_urine_analysis_sessions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sessions&t=us_id_positive_urine_analysis_sessions)
<br/>

#### Dependency Trees

##### Parentage
[sessions.us_id_positive_urine_analysis_sessions](../sessions/us_id_positive_urine_analysis_sessions.md) <br/>
|--us_id_raw_data_up_to_date_views.sbstnc_tst_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/sbstnc_tst.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=sbstnc_tst_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=sbstnc_tst_latest)) <br/>
|--us_id_raw_data_up_to_date_views.sbstnc_rslt_latest ([Raw Data Doc](../../../ingest/us_id/raw_data/sbstnc_rslt.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_id_raw_data_up_to_date_views&t=sbstnc_rslt_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_id_raw_data_up_to_date_views&t=sbstnc_rslt_latest)) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>


##### Descendants
[sessions.us_id_positive_urine_analysis_sessions](../sessions/us_id_positive_urine_analysis_sessions.md) <br/>
|--[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|----[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>
|--[case_triage.client_eligibility_criteria](../case_triage/client_eligibility_criteria.md) <br/>
|----[case_triage.etl_opportunities](../case_triage/etl_opportunities.md) <br/>
|------[linestaff_data_validation.recommended_downgrades](../linestaff_data_validation/recommended_downgrades.md) <br/>
|--------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|------[po_report_views.current_action_items_by_person](../po_report_views/current_action_items_by_person.md) <br/>
|--------[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|----------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|----------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|----------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|----------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|----------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|------[validation_views.case_triage_etl_freshness](../validation_views/case_triage_etl_freshness.md) <br/>

