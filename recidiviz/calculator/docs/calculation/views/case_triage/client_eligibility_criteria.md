## case_triage.client_eligibility_criteria
View containing attributes for current clients on supervision that may impact eligibility for decarceral opportunities.
    Currently this view supports the following attributes:
    - `case_type`
    - `supervision_level`
    - `days_served`: Number of days served on current supervision session
    - `prop_sentence_served`: Proportion of projected maximum sentence length served
    - `num_open_earned_discharge_requests`: Number of open (not rejected) earned discharge requests made
    - `days_at_current_supervision_level`: Days since last supervision level upgrade/downgrade
    - `days_since_last_positive_urine_analysis_date`
    - `positive_urine_analysis_results_past_year_count`
    - `is_employed`: CIS employment data contains a current job title/employer that does indicate unemployment
    - `last_verified_employment_date`: Most recent employment verification date available in CIS
    - `days_employed`: Number of days since person began any form of employment
    - `critical_contacts_count`: Count of number of emergency contacts in current supervision session
    

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=case_triage&t=client_eligibility_criteria)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=case_triage&t=client_eligibility_criteria)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id case_triage --view_id client_eligibility_criteria --show_downstream_dependencies False```

##### Descendants
[case_triage.client_eligibility_criteria](../case_triage/client_eligibility_criteria.md) <br/>
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

