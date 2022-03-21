## shared_metric_views.event_based_supervision_populations

 Supervision data on the person level with demographic information

 Expanded Dimensions: district, supervision_type
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=shared_metric_views&t=event_based_supervision_populations)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=shared_metric_views&t=event_based_supervision_populations)
<br/>

#### Dependency Trees

##### Parentage
[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|--[dashboard_views.ftr_referrals_by_age_by_period](../dashboard_views/ftr_referrals_by_age_by_period.md) <br/>
|----[validation_views.ftr_referrals_comparison](../validation_views/ftr_referrals_comparison.md) <br/>
|----[validation_views.ftr_referrals_comparison_errors](../validation_views/ftr_referrals_comparison_errors.md) <br/>
|--[dashboard_views.ftr_referrals_by_gender_by_period](../dashboard_views/ftr_referrals_by_gender_by_period.md) <br/>
|----[validation_views.ftr_referrals_comparison](../validation_views/ftr_referrals_comparison.md) <br/>
|----[validation_views.ftr_referrals_comparison_errors](../validation_views/ftr_referrals_comparison_errors.md) <br/>
|--[dashboard_views.ftr_referrals_by_lsir_by_period](../dashboard_views/ftr_referrals_by_lsir_by_period.md) <br/>
|----[validation_views.ftr_referrals_comparison](../validation_views/ftr_referrals_comparison.md) <br/>
|----[validation_views.ftr_referrals_comparison_errors](../validation_views/ftr_referrals_comparison_errors.md) <br/>
|--[dashboard_views.ftr_referrals_by_month](../dashboard_views/ftr_referrals_by_month.md) <br/>
|--[dashboard_views.ftr_referrals_by_period](../dashboard_views/ftr_referrals_by_period.md) <br/>
|--[dashboard_views.ftr_referrals_by_race_and_ethnicity_by_period](../dashboard_views/ftr_referrals_by_race_and_ethnicity_by_period.md) <br/>
|----[validation_views.ftr_referrals_comparison](../validation_views/ftr_referrals_comparison.md) <br/>
|----[validation_views.ftr_referrals_comparison_errors](../validation_views/ftr_referrals_comparison_errors.md) <br/>
|--[po_report_views.officer_supervision_district_association](../po_report_views/officer_supervision_district_association.md) <br/>
|----[analyst_data.event_based_metrics_by_supervision_officer](../analyst_data/event_based_metrics_by_supervision_officer.md) <br/>
|------[analyst_data.event_based_metrics_by_district](../analyst_data/event_based_metrics_by_district.md) <br/>
|----[experiments.case_triage_feedback_actions](../experiments/case_triage_feedback_actions.md) <br/>
|------[analyst_data.officer_events](../analyst_data/officer_events.md) <br/>
|------[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>
|----[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>
|----[experiments.officer_attributes](../experiments/officer_attributes.md) <br/>
|----[linestaff_data_validation.caseload_and_district](../linestaff_data_validation/caseload_and_district.md) <br/>
|------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|----[overdue_discharge_alert.overdue_discharge_alert_data](../overdue_discharge_alert/overdue_discharge_alert_data.md) <br/>
|----[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|------[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|------[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|------[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|------[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|------[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>
|--[public_dashboard_views.supervision_population_by_prioritized_race_and_ethnicity_by_period](../public_dashboard_views/supervision_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
|----[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|----[validation_views.supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency](../validation_views/supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency.md) <br/>
|----[validation_views.supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency_errors](../validation_views/supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency_errors.md) <br/>

