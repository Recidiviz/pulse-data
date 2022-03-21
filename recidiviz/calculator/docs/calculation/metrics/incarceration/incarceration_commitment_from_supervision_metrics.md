## IncarcerationCommitmentFromSupervisionMetric

The `IncarcerationCommitmentFromSupervisionMetric` stores information about all admissions to incarceration that qualify as a commitment from supervision. A commitment from supervision is when an individual that is on supervision is admitted to prison in response to a mandate from either the court or the parole board. This includes an admission to prison from supervision for any of the following reasons:

- Revocation of any kind (ex: probation, parole, community corrections, dual, etc) to serve a full/remainder of a sentence in prison (`REVOCATION`)
- Treatment mandated by either the court or the parole board (`SANCTION_ADMISSION`)
- Shock incarceration mandated by either the court or the parole board (`SANCTION_ADMISSION`)

Admissions to temporary parole board holds are not considered commitments from supervision admissions. If a person enters a parole board hold and then has their parole revoked by the parole board, then there will be a `IncarcerationCommitmentFromSupervisionMetric` for a `REVOCATION` on the date of the revocation.

With this metric, we can answer questions like:

- For all probation revocation admissions in 2013, what percent were from individuals supervised by District X?
- How have the admissions for treatment mandated by the parole board changed over time for individuals with an `ALCOHOL_DRUG` supervision case type?
- What was the distribution of supervision levels for all of the people admitted to prison due to a probation revocation in 2020?

This metric is a subset of the `IncarcerationAdmissionMetric`. This means that every admission in the `IncarcerationAdmissionMetric` output that qualifies as a commitment from supervision admission has a corresponding entry in the `IncarcerationCommitmentFromSupervisionMetrics`. This metric is used to track information about the supervision that preceded the admission to incarceration, as well as other information related to the type of commitment from supervision the admission represents. 

If a person was admitted to Facility X on 2021-01-01 for a `REVOCATION` from parole, then there will be an `IncarcerationAdmissionMetric` for this person on 2021-01-01 into Facility X and an associated `IncarcerationCommitmentFromSupervisionMetric` for this person on 2021-01-01 that stores all of the supervision information related to this commitment from supervision admission. If a person enters a parole board hold in Facility X on 2021-01-01, and then has their parole revoked by the parole board on 2021-02-13, then there will be an `IncarcerationAdmissionMetric` for the admission to the `PAROLE_BOARD_HOLD` in Facility X on 2021-01-01, another `IncarcerationAdmissionMetric` for the `REVOCATION` admission to Facility X on 2021-02-13, and an associated `IncarcerationCommitmentFromSupervisionMetric` for the `REVOCATION` on 2021-02-13 that stores all of the supervision information related to this commitment from supervision admission. The `supervision_type` for all of these metrics would be `PAROLE`.


#### Metric attributes
Attributes specific to the `IncarcerationCommitmentFromSupervisionMetric`:

|           **Attribute Name**           |**Type**|             **Enum Class**              |
|----------------------------------------|--------|-----------------------------------------|
|secondary_person_external_id            |STRING  |                                         |
|year                                    |INTEGER |                                         |
|month                                   |INTEGER |                                         |
|included_in_state_population            |BOOLEAN |                                         |
|facility                                |STRING  |                                         |
|county_of_residence                     |STRING  |                                         |
|admission_reason                        |STRING  |StateIncarcerationPeriodAdmissionReason  |
|admission_reason_raw_text               |STRING  |                                         |
|specialized_purpose_for_incarceration   |STRING  |StateSpecializedPurposeForIncarceration  |
|admission_date                          |DATE    |                                         |
|supervising_district_external_id        |STRING  |                                         |
|level_1_supervision_location_external_id|STRING  |                                         |
|level_2_supervision_location_external_id|STRING  |                                         |
|assessment_score_bucket                 |STRING  |                                         |
|assessment_type                         |STRING  |StateAssessmentType                      |
|most_severe_violation_type              |STRING  |StateSupervisionViolationType            |
|most_severe_violation_type_subtype      |STRING  |                                         |
|response_count                          |INTEGER |                                         |
|most_severe_response_decision           |STRING  |StateSupervisionViolationResponseDecision|
|purpose_for_incarceration_subtype       |STRING  |                                         |
|supervision_type                        |STRING  |StateSupervisionPeriodSupervisionType    |
|case_type                               |STRING  |StateSupervisionCaseType                 |
|supervision_level                       |STRING  |StateSupervisionLevel                    |
|supervision_level_raw_text              |STRING  |                                         |
|supervising_officer_external_id         |STRING  |                                         |
|violation_history_description           |STRING  |                                         |
|violation_type_frequency_counter        |STRING  |                                         |
|most_recent_response_decision           |STRING  |StateSupervisionViolationResponseDecision|


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=incarceration_commitment_from_supervision_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=incarceration_commitment_from_supervision_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[Maine](../../states/maine.md)              |                             36|daily                    |                |
|[Maine](../../states/maine.md)              |                            240|triggered by code changes|                |
|[Missouri](../../states/missouri.md)        |                             36|daily                    |                |
|[Missouri](../../states/missouri.md)        |                            240|triggered by code changes|                |
|[North Dakota](../../states/north_dakota.md)|                             36|daily                    |                |
|[North Dakota](../../states/north_dakota.md)|                            240|triggered by code changes|                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                            360|triggered by code changes|                |
|[Tennessee](../../states/tennessee.md)      |                             36|daily                    |                |
|[Tennessee](../../states/tennessee.md)      |                            240|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population --show_downstream_dependencies True```
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_commitment_from_supervision_metrics_not_included_in_state_population --show_downstream_dependencies True```

