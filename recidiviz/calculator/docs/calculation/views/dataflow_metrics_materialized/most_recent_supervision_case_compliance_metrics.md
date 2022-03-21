## dataflow_metrics_materialized.most_recent_supervision_case_compliance_metrics
supervision_case_compliance_metrics for the most recent job run

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_supervision_case_compliance_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_supervision_case_compliance_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_supervision_case_compliance_metrics](../dataflow_metrics_materialized/most_recent_supervision_case_compliance_metrics.md) <br/>
|--[dataflow_metrics.supervision_case_compliance_metrics](../../metrics/supervision/supervision_case_compliance_metrics.md) <br/>


##### Descendants
[dataflow_metrics_materialized.most_recent_supervision_case_compliance_metrics](../dataflow_metrics_materialized/most_recent_supervision_case_compliance_metrics.md) <br/>
|--[analyst_data.supervision_officer_caseload_health_metrics](../analyst_data/supervision_officer_caseload_health_metrics.md) <br/>
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
|--[case_triage.etl_opportunities](../case_triage/etl_opportunities.md) <br/>
|----[linestaff_data_validation.recommended_downgrades](../linestaff_data_validation/recommended_downgrades.md) <br/>
|------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|----[po_report_views.current_action_items_by_person](../po_report_views/current_action_items_by_person.md) <br/>
|------[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|--------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|--------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|--------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|--------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|--------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|----[validation_views.case_triage_etl_freshness](../validation_views/case_triage_etl_freshness.md) <br/>
|--[linestaff_data_validation.contacts_and_assessments](../linestaff_data_validation/contacts_and_assessments.md) <br/>
|----[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--[po_report_views.supervision_compliance_by_person_by_month](../po_report_views/supervision_compliance_by_person_by_month.md) <br/>
|----[po_report_views.report_data_by_person_by_month](../po_report_views/report_data_by_person_by_month.md) <br/>
|------[linestaff_data_validation.violations](../linestaff_data_validation/violations.md) <br/>
|--------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|------[po_report_views.report_data_by_officer_by_month](../po_report_views/report_data_by_officer_by_month.md) <br/>
|--------[linestaff_data_validation.metrics_from_po_report](../linestaff_data_validation/metrics_from_po_report.md) <br/>
|--------[linestaff_data_validation.po_events](../linestaff_data_validation/po_events.md) <br/>
|--------[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|----------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|----------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|----------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|----------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|----------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|------[validation_views.case_termination_by_type_comparison](../validation_views/case_termination_by_type_comparison.md) <br/>
|------[validation_views.case_termination_by_type_comparisonabsconsions_errors](../validation_views/case_termination_by_type_comparisonabsconsions_errors.md) <br/>
|------[validation_views.case_termination_by_type_comparisondischarges_errors](../validation_views/case_termination_by_type_comparisondischarges_errors.md) <br/>
|--[sessions.supervision_downgrade_sessions](../sessions/supervision_downgrade_sessions.md) <br/>
|----[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|------[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>
|----[analyst_data.person_statuses](../analyst_data/person_statuses.md) <br/>
|--[shared_metric_views.supervision_case_compliance_metrics](../shared_metric_views/supervision_case_compliance_metrics.md) <br/>
|----[shared_metric_views.supervision_mismatches_by_day](../shared_metric_views/supervision_mismatches_by_day.md) <br/>
|------[vitals_report_views.supervision_downgrade_opportunities_by_po_by_day](../vitals_report_views/supervision_downgrade_opportunities_by_po_by_day.md) <br/>
|--------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|--------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|----[vitals_report_views.overdue_lsir_by_po_by_day](../vitals_report_views/overdue_lsir_by_po_by_day.md) <br/>
|------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|----[vitals_report_views.timely_contact_by_po_by_day](../vitals_report_views/timely_contact_by_po_by_day.md) <br/>
|------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>

