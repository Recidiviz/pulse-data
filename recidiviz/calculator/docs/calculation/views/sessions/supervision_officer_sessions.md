## sessions.supervision_officer_sessions
Sessionized view of each individual. Session defined as continuous stay on supervision associated with a given officer. Officer sessions may be overlapping.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sessions&t=supervision_officer_sessions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sessions&t=supervision_officer_sessions)
<br/>

#### Dependency Trees

##### Parentage
[sessions.supervision_officer_sessions](../sessions/supervision_officer_sessions.md) <br/>
|--[sessions.dataflow_sessions](../sessions/dataflow_sessions.md) <br/>
|----[sessions.us_tn_judicial_district_sessions](../sessions/us_tn_judicial_district_sessions.md) <br/>
|------[sessions.us_tn_sentences_preprocessed](../sessions/us_tn_sentences_preprocessed.md) <br/>
|--------us_tn_raw_data_up_to_date_views.Sentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/Sentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) <br/>
|--------us_tn_raw_data_up_to_date_views.SentenceMiscellaneous_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/SentenceMiscellaneous.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) <br/>
|--------us_tn_raw_data_up_to_date_views.OffenderStatute_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/OffenderStatute.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) <br/>
|--------us_tn_raw_data_up_to_date_views.JOSentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOSentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) <br/>
|--------us_tn_raw_data_up_to_date_views.JOIdentification_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOIdentification.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) <br/>
|--------us_tn_raw_data_up_to_date_views.JOCharge_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOCharge.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) <br/>
|--------state.state_supervision_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_sentence)) <br/>
|--------state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|--------state.state_incarceration_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_sentence)) <br/>
|----[sessions.us_tn_incarceration_population_metrics_preprocessed](../sessions/us_tn_incarceration_population_metrics_preprocessed.md) <br/>
|------state.state_incarceration_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_period)) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|----[sessions.us_mo_supervision_population_metrics_preprocessed](../sessions/us_mo_supervision_population_metrics_preprocessed.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|----[sessions.us_id_supervision_population_metrics_preprocessed](../sessions/us_id_supervision_population_metrics_preprocessed.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|----[sessions.us_id_supervision_out_of_state_population_metrics_preprocessed](../sessions/us_id_supervision_out_of_state_population_metrics_preprocessed.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|----[sessions.us_id_incarceration_population_metrics_preprocessed](../sessions/us_id_incarceration_population_metrics_preprocessed.md) <br/>
|------static_reference_tables.state_incarceration_facilities ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_incarceration_facilities)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_incarceration_facilities)) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_not_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>


##### Descendants
[sessions.supervision_officer_sessions](../sessions/supervision_officer_sessions.md) <br/>
|--[analyst_data.event_based_metrics_by_supervision_officer](../analyst_data/event_based_metrics_by_supervision_officer.md) <br/>
|----[analyst_data.event_based_metrics_by_district](../analyst_data/event_based_metrics_by_district.md) <br/>
|--[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|----[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>
|--[analyst_data.supervision_officer_caseload_health_metrics](../analyst_data/supervision_officer_caseload_health_metrics.md) <br/>
|--[analyst_data.supervision_population_by_officer_daily_windows](../analyst_data/supervision_population_by_officer_daily_windows.md) <br/>
|----[analyst_data.event_based_metrics_by_supervision_officer](../analyst_data/event_based_metrics_by_supervision_officer.md) <br/>
|------[analyst_data.event_based_metrics_by_district](../analyst_data/event_based_metrics_by_district.md) <br/>
|--[case_triage.etl_clients](../case_triage/etl_clients.md) <br/>
|----[case_triage.client_eligibility_criteria](../case_triage/client_eligibility_criteria.md) <br/>
|------[case_triage.etl_opportunities](../case_triage/etl_opportunities.md) <br/>
|--------[linestaff_data_validation.recommended_downgrades](../linestaff_data_validation/recommended_downgrades.md) <br/>
|----------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--------[po_report_views.current_action_items_by_person](../po_report_views/current_action_items_by_person.md) <br/>
|----------[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|------------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|------------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|------------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|------------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|------------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|--------[validation_views.case_triage_etl_freshness](../validation_views/case_triage_etl_freshness.md) <br/>
|----[case_triage.etl_client_events](../case_triage/etl_client_events.md) <br/>
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
|----[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>
|----[po_report_views.current_action_items_by_person](../po_report_views/current_action_items_by_person.md) <br/>
|------[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|--------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|--------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|--------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|--------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|--------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|----[validation_views.case_triage_etl_freshness](../validation_views/case_triage_etl_freshness.md) <br/>
|----[validation_views.case_triage_f2f_contact_freshness](../validation_views/case_triage_f2f_contact_freshness.md) <br/>
|----[validation_views.case_triage_risk_assessment_freshness](../validation_views/case_triage_risk_assessment_freshness.md) <br/>
|----[validation_views.most_recent_assessment_date_by_person_by_state_comparison](../validation_views/most_recent_assessment_date_by_person_by_state_comparison.md) <br/>
|----[validation_views.most_recent_assessment_date_by_person_by_state_comparison_errors](../validation_views/most_recent_assessment_date_by_person_by_state_comparison_errors.md) <br/>
|----[validation_views.most_recent_assessment_score_by_person_by_state_comparison](../validation_views/most_recent_assessment_score_by_person_by_state_comparison.md) <br/>
|----[validation_views.most_recent_assessment_score_by_person_by_state_comparison_errors](../validation_views/most_recent_assessment_score_by_person_by_state_comparison_errors.md) <br/>
|----[validation_views.most_recent_face_to_face_contact_date_by_person_by_state_comparison](../validation_views/most_recent_face_to_face_contact_date_by_person_by_state_comparison.md) <br/>
|----[validation_views.most_recent_face_to_face_contact_date_by_person_by_state_comparison_errors](../validation_views/most_recent_face_to_face_contact_date_by_person_by_state_comparison_errors.md) <br/>
|--[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>
|--[linestaff_data_validation.caseload_and_district](../linestaff_data_validation/caseload_and_district.md) <br/>
|----[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--[linestaff_data_validation.metrics_from_po_report](../linestaff_data_validation/metrics_from_po_report.md) <br/>
|--[linestaff_data_validation.po_events](../linestaff_data_validation/po_events.md) <br/>
|--[sessions.supervision_tool_access_sessions](../sessions/supervision_tool_access_sessions.md) <br/>
|----[sessions.supervision_downgrade_sessions](../sessions/supervision_downgrade_sessions.md) <br/>
|------[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|--------[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>
|------[analyst_data.person_statuses](../analyst_data/person_statuses.md) <br/>

