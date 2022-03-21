## SupervisionOutOfStatePopulationMetric

The `SupervisionOutOfStatePopulationMetric` stores information about a single day that an individual spent on supervision, where the person is serving their supervision in another state. This metric tracks each day on which an individual was counted in the state’s “out of state” supervision population, and includes information related to the period of supervision. 

With this metric, we can answer questions like:

- How many people are actively serving their US_XX supervision in a state other than US_XX?
- How has the number of people serving probation in another state changed over time for state US_YY?
- What proportion of individuals serving parole in another state in 2010 were men?

This metric is mutually exclusive from the `SupervisionPopulationMetric`. If a person is in the `SupervisionOutOfStatePopulationMetric` on a given day then they will not be counted in the `SupervisionPopulationMetric` for that day.

This metric is derived from the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. All population metrics are end date exclusive, meaning a person is not counted in the supervision population on the day that their supervision is terminated. The population metrics are start date inclusive, meaning that a person is counted in the population on the date that they start supervision.

A person is excluded from the out of state supervision population on any days that they are also incarcerated, unless that incarceration is under the custodial authority of the supervision authority in the state. For example, say a person is incarcerated on 2021-01-01, where the custodial authority of the incarceration is the `STATE_PRISON`. If this person is simultaneously serving a probation term (that they have been serving while out of the state) while they are incarcerated, they will not be counted in the out of state supervision population because they are in prison under the state prison custodial authority. However, if instead the custodial authority of this incarceration is the `SUPERVISION_AUTHORITY` of the state, then this incarceration will not exclude the person from being counted in the out of state supervision population on this day. 


#### Metric attributes
Attributes specific to the `SupervisionOutOfStatePopulationMetric`:

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
|assessment_score_bucket                 |STRING  |                                         |
|assessment_type                         |STRING  |StateAssessmentType                      |
|date_of_supervision                     |DATE    |                                         |
|projected_end_date                      |DATE    |                                         |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=supervision_out_of_state_population_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=supervision_out_of_state_population_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[Missouri](../../states/missouri.md)        |                            240|triggered by code changes|                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                            240|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_out_of_state_population_metrics --show_downstream_dependencies True```

