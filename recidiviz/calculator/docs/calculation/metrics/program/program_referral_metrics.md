## ProgramReferralMetric

The `ProgramReferralMetric` stores information about a person getting referred to rehabilitative programming. This metric tracks the day that a person was referred by someone (usually a correctional or supervision officer) to a given program, and stores information related to that referral.

With this metric, we can answer questions like:

- Of all of the people referred to Program X last month, how many are now actively participating in the program?
- How many people have been referred to Program Y since it was introduced in January 2017?
- Which supervision district referred the most people to Program Z in 2020?

 
This metric is derived from the `StateProgramAssignment` entities, which store information about the assignment of a person to some form of rehabilitative programming -- and their participation in the program -- intended to address specific needs of the person. The calculations for this metric look for `StateProgramAssignment` instances with a `referral_date` to determine that a program referral occurred. 

If a person was referred to Program X on April 1, 2020, then there will be a `ProgramReferralMetric` for April 1, 2020.

If a person was referred to a program while they were on supervision, then this metric records information about the supervision the person was on on the date of the referral. If a person is serving multiple supervisions simultaneously (and has multiple `StateSupervisionPeriod` entities that overlap a referral date) then there will be one `ProgramReferralMetric` produced for each overlapping supervision period. So, if a person was referred to Program Z on December 3, 2014, and on that day the person was serving both probation and parole simultaneously (represented by two overlapping `StateSupervisionPeriod` entities), then there will be two `ProgramReferralMetrics` produced: one with a `supervision_type` of `PAROLE` and one with a `supervision_type` of `PROBATION`.


#### Metric attributes
Attributes specific to the `ProgramReferralMetric`:

|           **Attribute Name**           |**Type**|             **Enum Class**              |
|----------------------------------------|--------|-----------------------------------------|
|year                                    |INTEGER |                                         |
|month                                   |INTEGER |                                         |
|program_id                              |STRING  |                                         |
|assessment_score_bucket                 |STRING  |                                         |
|assessment_type                         |STRING  |StateAssessmentType                      |
|supervising_district_external_id        |STRING  |                                         |
|level_1_supervision_location_external_id|STRING  |                                         |
|level_2_supervision_location_external_id|STRING  |                                         |
|date_of_referral                        |DATE    |                                         |
|supervision_type                        |STRING  |StateSupervisionPeriodSupervisionType    |
|participation_status                    |STRING  |StateProgramAssignmentParticipationStatus|
|supervising_officer_external_id         |STRING  |                                         |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=program_referral_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=program_referral_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[North Dakota](../../states/north_dakota.md)|                             36|daily                    |                |
|[North Dakota](../../states/north_dakota.md)|                             60|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_program_referral_metrics --show_downstream_dependencies True```

