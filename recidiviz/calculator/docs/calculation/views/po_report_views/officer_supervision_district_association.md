## po_report_views.officer_supervision_district_association

 Officer supervision district association.
 Identifies the district in which a given parole officer has the largest number of cases.
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=po_report_views&t=officer_supervision_district_association)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=po_report_views&t=officer_supervision_district_association)
<br/>

#### Dependency Trees

##### Parentage
[po_report_views.officer_supervision_district_association](../po_report_views/officer_supervision_district_association.md) <br/>
|--[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
[po_report_views.officer_supervision_district_association](../po_report_views/officer_supervision_district_association.md) <br/>
|--[analyst_data.event_based_metrics_by_supervision_officer](../analyst_data/event_based_metrics_by_supervision_officer.md) <br/>
|----[analyst_data.event_based_metrics_by_district](../analyst_data/event_based_metrics_by_district.md) <br/>
|--[experiments.case_triage_feedback_actions](../experiments/case_triage_feedback_actions.md) <br/>
|----[analyst_data.officer_events](../analyst_data/officer_events.md) <br/>
|----[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>
|--[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>
|--[experiments.officer_attributes](../experiments/officer_attributes.md) <br/>
|--[linestaff_data_validation.caseload_and_district](../linestaff_data_validation/caseload_and_district.md) <br/>
|----[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--[overdue_discharge_alert.overdue_discharge_alert_data](../overdue_discharge_alert/overdue_discharge_alert_data.md) <br/>
|--[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|----[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|----[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|----[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|----[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|----[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>

