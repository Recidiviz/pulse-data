## dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics
supervision_population_metrics output for the most recent single day recorded for this metric

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_single_day_supervision_population_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_single_day_supervision_population_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_single_day_supervision_population_metrics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_single_day_supervision_population_metrics.md) <br/>
|--[analyst_data.projected_discharges](../analyst_data/projected_discharges.md) <br/>
|----[analyst_data.us_pa_raw_required_treatment](../analyst_data/us_pa_raw_required_treatment.md) <br/>
|----[linestaff_data_validation.overdue_discharges](../linestaff_data_validation/overdue_discharges.md) <br/>
|------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|----[overdue_discharge_alert.overdue_discharge_alert_data](../overdue_discharge_alert/overdue_discharge_alert_data.md) <br/>
|----[shared_metric_views.overdue_discharge_alert_exclusions](../shared_metric_views/overdue_discharge_alert_exclusions.md) <br/>
|------[overdue_discharge_alert.overdue_discharge_alert_data](../overdue_discharge_alert/overdue_discharge_alert_data.md) <br/>
|--[analyst_data.us_pa_raw_required_treatment](../analyst_data/us_pa_raw_required_treatment.md) <br/>
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
|--[dashboard_views.supervision_population_snapshot_by_dimension](../dashboard_views/supervision_population_snapshot_by_dimension.md) <br/>
|--[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|--[linestaff_data_validation.caseload_and_district](../linestaff_data_validation/caseload_and_district.md) <br/>
|----[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--[shared_metric_views.single_day_supervision_population_for_spotlight](../shared_metric_views/single_day_supervision_population_for_spotlight.md) <br/>
|----[public_dashboard_views.sentence_type_by_district_by_demographics](../public_dashboard_views/sentence_type_by_district_by_demographics.md) <br/>
|------[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|------[validation_views.sentence_type_by_district_by_demographics_internal_consistency](../validation_views/sentence_type_by_district_by_demographics_internal_consistency.md) <br/>
|------[validation_views.sentence_type_by_district_by_demographics_internal_consistency_errors](../validation_views/sentence_type_by_district_by_demographics_internal_consistency_errors.md) <br/>
|----[public_dashboard_views.supervision_population_by_district_by_demographics](../public_dashboard_views/supervision_population_by_district_by_demographics.md) <br/>
|------[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|------[validation_views.supervision_population_by_district_by_demographics_internal_consistency](../validation_views/supervision_population_by_district_by_demographics_internal_consistency.md) <br/>
|------[validation_views.supervision_population_by_district_by_demographics_internal_consistency_errors](../validation_views/supervision_population_by_district_by_demographics_internal_consistency_errors.md) <br/>

