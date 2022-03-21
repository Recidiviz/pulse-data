## SupervisionSuccessMetric

The `SupervisionSuccessMetric` stores information about whether supervision that was projected to end in a given month was completed successfully. This metric tracks the month in which supervision was scheduled to be completed, whether the completion was successful, and other information about the supervision that was completed.

With this metric, we can answer questions like:

- Of all of the people that were supposed to complete supervision last month, what percent completed it successfully?
- How has the rate of successful completion of supervision changed over time?
- Do the rates of successful completion of supervision vary by the race of the person on supervision?

The calculations for this metric use both `StateSupervisionSentence` and `StateIncarcerationSentence` entities to determine projected supervision completion dates, and rely on related `StateSupervisionPeriod` entities to determine whether or not the supervision was completed successfully. 

In order for a sentence to be included in these calculations it must:

- Have a set `start_date`, indicating that the sentence was started
- Have a set `completion_date`, indicating that the sentence has been completed
- Have the values required to calculate the projected date of completion (`projected_completion_date` for supervision sentences, and `max_length_days` for incarceration sentences)
- Have supervision periods with `start_date` and `termination_date` values that are within the bounds of the sentence (start and end dates inclusive here)

If a sentence meets these criteria, then the `termination_reason` on the `StateSupervisionPeriod` with the latest `termination_date` is used to determine whether the sentence was completed successfully. For example, if the last supervision period that falls within the bounds of a sentence is terminated with a reason of `DISCHARGE`, then this is marked as a successful completion. If the last supervision period within the bounds of a sentence is terminated with a reason of `REVOCATION`, then this is marked as an unsuccessful completion.

If a person has a `StateSupervisionSentence` with a `projected_completion_date` of 2020-02-28 and a `completion_date` of 2020-02-18, has one `StateSupervisionPeriod` that terminated on 2018-03-13 with a reason of `REVOCATION`, and another `StateSupervisionPeriod` that terminated on 2020-02-17 with a reason of `DISCHARGE`, then there would be a `SupervisionSuccessMetric` for the `year=2020`, the `month=02`, with the `successful_completion` boolean marked as `True`. If a person has a `StateIncarcerationSentence` with a `start_date` of 2019-01-01 and a `max_length_days` value of `800`, then their projected date of completion is 2021-03-11. If their latest `StateSupervisionPeriod` within the bounds of this sentence was terminated on 2020-11-18 with a reason of `REVOCATION`, then there would be a `SupervisionSuccessMetric` for the `year=2021` and `month=03`, where the `successful_completion` is marked as `False`.


#### Metric attributes
Attributes specific to the `SupervisionSuccessMetric`:

|           **Attribute Name**           |**Type**|           **Enum Class**            |
|----------------------------------------|--------|-------------------------------------|
|supervising_district_external_id        |STRING  |                                     |
|level_1_supervision_location_external_id|STRING  |                                     |
|level_2_supervision_location_external_id|STRING  |                                     |
|year                                    |INTEGER |                                     |
|month                                   |INTEGER |                                     |
|supervision_type                        |STRING  |StateSupervisionPeriodSupervisionType|
|case_type                               |STRING  |StateSupervisionCaseType             |
|supervision_level                       |STRING  |StateSupervisionLevel                |
|supervision_level_raw_text              |STRING  |                                     |
|supervising_officer_external_id         |STRING  |                                     |
|judicial_district_code                  |STRING  |                                     |
|custodial_authority                     |STRING  |StateCustodialAuthority              |
|successful_completion                   |BOOLEAN |                                     |
|sentence_days_served                    |INTEGER |                                     |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=supervision_success_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=supervision_success_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[North Dakota](../../states/north_dakota.md)|                             36|daily                    |                |
|[North Dakota](../../states/north_dakota.md)|                            240|triggered by code changes|                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_success_metrics --show_downstream_dependencies True```

