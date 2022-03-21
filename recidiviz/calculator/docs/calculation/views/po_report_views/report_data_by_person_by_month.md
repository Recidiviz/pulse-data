## po_report_views.report_data_by_person_by_month

 Person-level data regarding early discharges, successful supervision completions, reported recommendations for
 absconsions and revocations, and case compliance statuses.
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=po_report_views&t=report_data_by_person_by_month)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=po_report_views&t=report_data_by_person_by_month)
<br/>

#### Dependency Trees

##### Parentage
[po_report_views.report_data_by_person_by_month](../po_report_views/report_data_by_person_by_month.md) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|--state.state_person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person)) <br/>
|--[po_report_views.supervision_earned_discharge_requests_by_person_by_month](../po_report_views/supervision_earned_discharge_requests_by_person_by_month.md) <br/>
|----state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----state.state_early_discharge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_early_discharge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_early_discharge)) <br/>
|----[reference_views.supervision_period_to_agent_association](../reference_views/supervision_period_to_agent_association.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|--------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
|--[po_report_views.supervision_downgrade_by_person_by_month](../po_report_views/supervision_downgrade_by_person_by_month.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_downgrade_metrics](../dataflow_metrics_materialized/most_recent_supervision_downgrade_metrics.md) <br/>
|------[dataflow_metrics.supervision_downgrade_metrics](../../metrics/supervision/supervision_downgrade_metrics.md) <br/>
|--[po_report_views.supervision_compliance_by_person_by_month](../po_report_views/supervision_compliance_by_person_by_month.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_case_compliance_metrics](../dataflow_metrics_materialized/most_recent_supervision_case_compliance_metrics.md) <br/>
|------[dataflow_metrics.supervision_case_compliance_metrics](../../metrics/supervision/supervision_case_compliance_metrics.md) <br/>
|--[po_report_views.successful_supervision_completions_by_person_by_month](../po_report_views/successful_supervision_completions_by_person_by_month.md) <br/>
|----state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----[reference_views.supervision_period_to_agent_association](../reference_views/supervision_period_to_agent_association.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|--------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
|--[po_report_views.revocation_reports_by_person_by_month](../po_report_views/revocation_reports_by_person_by_month.md) <br/>
|----state.state_supervision_violation_type_entry ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_violation_type_entry)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_violation_type_entry)) <br/>
|----state.state_supervision_violation_response_decision_entry ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_violation_response_decision_entry)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_violation_response_decision_entry)) <br/>
|----state.state_supervision_violation_response ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_violation_response)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_violation_response)) <br/>
|----state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----[reference_views.supervision_period_to_agent_association](../reference_views/supervision_period_to_agent_association.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|--------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
|--[po_report_views.absconsion_reports_by_person_by_month](../po_report_views/absconsion_reports_by_person_by_month.md) <br/>
|----state.state_supervision_violation_type_entry ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_violation_type_entry)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_violation_type_entry)) <br/>
|----state.state_supervision_violation_response_decision_entry ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_violation_response_decision_entry)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_violation_response_decision_entry)) <br/>
|----state.state_supervision_violation_response ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_violation_response)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_violation_response)) <br/>
|----state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----[reference_views.supervision_period_to_agent_association](../reference_views/supervision_period_to_agent_association.md) <br/>
|------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|--------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>


##### Descendants
[po_report_views.report_data_by_person_by_month](../po_report_views/report_data_by_person_by_month.md) <br/>
|--[linestaff_data_validation.violations](../linestaff_data_validation/violations.md) <br/>
|----[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--[po_report_views.report_data_by_officer_by_month](../po_report_views/report_data_by_officer_by_month.md) <br/>
|----[linestaff_data_validation.metrics_from_po_report](../linestaff_data_validation/metrics_from_po_report.md) <br/>
|----[linestaff_data_validation.po_events](../linestaff_data_validation/po_events.md) <br/>
|----[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|--[validation_views.case_termination_by_type_comparison](../validation_views/case_termination_by_type_comparison.md) <br/>
|--[validation_views.case_termination_by_type_comparisonabsconsions_errors](../validation_views/case_termination_by_type_comparisonabsconsions_errors.md) <br/>
|--[validation_views.case_termination_by_type_comparisondischarges_errors](../validation_views/case_termination_by_type_comparisondischarges_errors.md) <br/>

