## experiments.case_triage_feedback_actions

All case feedback actions taken by an officer within the Case Triage app for an
individual client on a single day (day boundary defined in the PST time zone).
Case feedback can include updates around a client's data quality (incorrect data),
assessment information, supervision downgrades or discharges, etc.
Case feedback that is removed/reverted within 3 days of the original feedback is
excluded from the results.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=experiments&t=case_triage_feedback_actions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=experiments&t=case_triage_feedback_actions)
<br/>

#### Dependency Trees

##### Parentage
[experiments.case_triage_feedback_actions](../experiments/case_triage_feedback_actions.md) <br/>
|--static_reference_tables.case_triage_users ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=case_triage_users)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=case_triage_users)) <br/>
|--[sessions.person_demographics](../sessions/person_demographics.md) <br/>
|----static_reference_tables.state_race_ethnicity_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_race_ethnicity_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_race_ethnicity_population_counts)) <br/>
|----state.state_person_race ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_race)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_race)) <br/>
|----state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|----state.state_person_ethnicity ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_ethnicity)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_ethnicity)) <br/>
|----state.state_person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person)) <br/>
|--[po_report_views.officer_supervision_district_association](../po_report_views/officer_supervision_district_association.md) <br/>
|----[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--case_triage_segment_metrics.frontend_person_action_taken ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=case_triage_segment_metrics&t=frontend_person_action_taken)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=case_triage_segment_metrics&t=frontend_person_action_taken)) <br/>
|--case_triage_segment_metrics.frontend_person_action_removed ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=case_triage_segment_metrics&t=frontend_person_action_removed)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=case_triage_segment_metrics&t=frontend_person_action_removed)) <br/>


##### Descendants
[experiments.case_triage_feedback_actions](../experiments/case_triage_feedback_actions.md) <br/>
|--[analyst_data.officer_events](../analyst_data/officer_events.md) <br/>
|--[experiments.case_triage_metrics](../experiments/case_triage_metrics.md) <br/>

