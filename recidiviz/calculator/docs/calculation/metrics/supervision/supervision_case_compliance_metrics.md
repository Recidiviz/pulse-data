## SupervisionCaseComplianceMetric

The `SupervisionCaseComplianceMetric` is a subclass of the `SupervisionPopulationMetric` that stores information related to the compliance standards of the supervision case for a day that a person is counted in the supervision population. This metric records information relevant to the contact and assessment frequency required for the supervision case, and whether certain case compliance requirements are being met on the `date_of_evaluation`.

With this metric, we can answer questions like:

- What percent of people in the supervision population have supervision cases that are meeting the face-to-face contact standard for the state?
- At the end of last month how many people were overdue on having an assessment done on them?
- What proportion of individuals on Officer X’s caseload are actively eligible for a supervision downgrade?

This metric is derived from the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised, the `StateAssessment` entities, which store instances of when a person was assessed, and `StateSupervisionContact` entities, which store information about when a supervision officer makes contact with a person on supervision (or with someone in the person’s community). 

The calculation of the attributes of this metric relies entirely on the state-specific implementation of the `StateSupervisionCaseComplianceManager`. All compliance guidelines are state-specific, and the details of these state-specific calculations can be found in the file with the `StateSupervisionCaseComplianceManager` implementation for each state.


#### Metric attributes
Attributes specific to the `SupervisionCaseComplianceMetric`:

|                **Attribute Name**                |**Type**|             **Enum Class**              |
|--------------------------------------------------|--------|-----------------------------------------|
|supervising_district_external_id                  |STRING  |                                         |
|level_1_supervision_location_external_id          |STRING  |                                         |
|level_2_supervision_location_external_id          |STRING  |                                         |
|year                                              |INTEGER |                                         |
|month                                             |INTEGER |                                         |
|supervision_type                                  |STRING  |StateSupervisionPeriodSupervisionType    |
|case_type                                         |STRING  |StateSupervisionCaseType                 |
|supervision_level                                 |STRING  |StateSupervisionLevel                    |
|supervision_level_raw_text                        |STRING  |                                         |
|supervising_officer_external_id                   |STRING  |                                         |
|judicial_district_code                            |STRING  |                                         |
|custodial_authority                               |STRING  |StateCustodialAuthority                  |
|most_severe_violation_type                        |STRING  |StateSupervisionViolationType            |
|most_severe_violation_type_subtype                |STRING  |                                         |
|response_count                                    |INTEGER |                                         |
|most_severe_response_decision                     |STRING  |StateSupervisionViolationResponseDecision|
|assessment_score_bucket                           |STRING  |                                         |
|assessment_type                                   |STRING  |StateAssessmentType                      |
|date_of_supervision                               |DATE    |                                         |
|projected_end_date                                |DATE    |                                         |
|date_of_evaluation                                |DATE    |                                         |
|assessment_count                                  |INTEGER |                                         |
|face_to_face_count                                |INTEGER |                                         |
|home_visit_count                                  |INTEGER |                                         |
|most_recent_assessment_date                       |DATE    |                                         |
|next_recommended_assessment_date                  |DATE    |                                         |
|most_recent_face_to_face_date                     |DATE    |                                         |
|next_recommended_face_to_face_date                |DATE    |                                         |
|most_recent_home_visit_date                       |DATE    |                                         |
|next_recommended_home_visit_date                  |DATE    |                                         |
|most_recent_treatment_collateral_contact_date     |DATE    |                                         |
|next_recommended_treatment_collateral_contact_date|DATE    |                                         |
|recommended_supervision_downgrade_level           |STRING  |StateSupervisionLevel                    |


Attributes on all metrics:

|     **Attribute Name**      |**Type**|  **Enum Class**   |
|-----------------------------|--------|-------------------|
|job_id                       |STRING  |                   |
|state_code                   |STRING  |                   |
|age                          |INTEGER |                   |
|prioritized_race_or_ethnicity|STRING  |                   |
|gender                       |STRING  |Gender             |
|created_on                   |DATE    |                   |
|updated_on                   |DATE    |                   |
|person_id                    |INTEGER |                   |
|person_external_id           |STRING  |                   |
|metric_type                  |STRING  |RecidivizMetricType|


#### Metric tables in BigQuery

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=supervision_case_compliance_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=supervision_case_compliance_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[North Dakota](../../states/north_dakota.md)|                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_case_compliance_metrics --show_downstream_dependencies True```

