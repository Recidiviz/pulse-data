## SupervisionTerminationMetric

The `SupervisionTerminationMetric` stores information about when a person ends supervision. This metric tracks the date on which an individual ended a period of supervision, and includes information related to the period of supervision that ended.

With this metric, we can answer questions like:

- How many periods of supervision ended this month because the person absconded?
- How many people finished serving their supervision terms this year because they were discharged from their supervision earlier than their sentence’s end date?
- How many people on Officer X’s caseload this month had their supervisions successfully terminated?

This metric is derived from the `termination_date` on `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. A `SupervisionTerminationMetric` is created for the day that a person ended a supervision period, even if the person was incarcerated on the date the supervision ended. There are two attributes (`in_incarceration_population_on_date` and `in_supervision_population_on_date`) on this metric that indicate whether the person was counted in the incarceration and/or supervision population on the date that the supervision ended.

The presence of this metric does not mean that a person has fully completed their supervision. There is a `SupervisionTerminationMetric` for all terminations of supervision, regardless of the `termination_reason` on the period (for example, there are many terminations with a `termination_reason` of `TRANSFER_WITHIN_STATE` that mark when a person is being transferred from one supervising officer to another). 

If a person has a supervision period with a `termination_date` of 2017-10-18, then there will be a `SupervisionTerminationMetric` with a `termination_date` of 2017-10-18 that include all details of the supervision that was started. 


#### Metric attributes
Attributes specific to the `SupervisionTerminationMetric`:

|           **Attribute Name**           |**Type**|             **Enum Class**              |
|----------------------------------------|--------|-----------------------------------------|
|supervising_district_external_id        |STRING  |                                         |
|level_1_supervision_location_external_id|STRING  |                                         |
|level_2_supervision_location_external_id|STRING  |                                         |
|year                                    |INTEGER |                                         |
|month                                   |INTEGER |                                         |
|supervision_type                        |STRING  |StateSupervisionPeriodSupervisionType    |
|case_type                               |STRING  |StateSupervisionCaseType                 |
|supervision_level                       |STRING  |StateSupervisionLevel                    |
|supervision_level_raw_text              |STRING  |                                         |
|supervising_officer_external_id         |STRING  |                                         |
|judicial_district_code                  |STRING  |                                         |
|custodial_authority                     |STRING  |StateCustodialAuthority                  |
|most_severe_violation_type              |STRING  |StateSupervisionViolationType            |
|most_severe_violation_type_subtype      |STRING  |                                         |
|response_count                          |INTEGER |                                         |
|most_severe_response_decision           |STRING  |StateSupervisionViolationResponseDecision|
|in_incarceration_population_on_date     |BOOLEAN |                                         |
|in_supervision_population_on_date       |BOOLEAN |                                         |
|assessment_score_bucket                 |STRING  |                                         |
|assessment_type                         |STRING  |StateAssessmentType                      |
|assessment_score_change                 |FLOAT   |                                         |
|termination_reason                      |STRING  |StateSupervisionPeriodTerminationReason  |
|termination_date                        |DATE    |                                         |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=supervision_termination_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=supervision_termination_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[Missouri](../../states/missouri.md)        |                             36|daily                    |                |
|[Missouri](../../states/missouri.md)        |                            240|triggered by code changes|                |
|[North Dakota](../../states/north_dakota.md)|                             36|daily                    |                |
|[North Dakota](../../states/north_dakota.md)|                            240|triggered by code changes|                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                            240|triggered by code changes|                |
|[Tennessee](../../states/tennessee.md)      |                             36|daily                    |                |
|[Tennessee](../../states/tennessee.md)      |                            240|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_termination_metrics --show_downstream_dependencies True```

