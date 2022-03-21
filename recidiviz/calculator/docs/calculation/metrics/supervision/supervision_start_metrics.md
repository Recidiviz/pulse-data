## SupervisionStartMetric

The `SupervisionStartMetric` stores information about when a person starts supervision. This metric tracks the date on which an individual started a period of supervision, and includes information related to the period of supervision that was started.

With this metric, we can answer questions like:

- How many people started serving probation due to a new court sentence this year?
- How many people started being supervised by Officer X this month?
- What proportion of individuals that started serving parole in 2020 were white men?

This metric is derived from the `start_date` on `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. A `SupervisionStartMetric` is created for the day that a person started a supervision period, even if the person was incarcerated on the date the supervision started. There are two attributes (`in_incarceration_population_on_date` and `in_supervision_population_on_date`) on this metric that indicate whether the person was counted in the incarceration and/or supervision population on the date that the supervision started.

The presence of this metric does not mean that a person is starting to serve a new sentence of supervision. There is a `SupervisionStartMetric` for all starts of supervision, regardless of the  `admission_reason` on the period (for example, there are many starts with a `admission_reason` of `TRANSFER_WITHIN_STATE` that mark when a person is being transferred from one supervising officer to another). 

If a person has a supervision period with a `start_date` of 2017-10-18, then there will be a `SupervisionStartMetric` with a `start_date` of 2017-10-18 that include all details of the supervision that was started. 


#### Metric attributes
Attributes specific to the `SupervisionStartMetric`:

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
|in_incarceration_population_on_date     |BOOLEAN |                                     |
|in_supervision_population_on_date       |BOOLEAN |                                     |
|is_official_supervision_admission       |BOOLEAN |                                     |
|admission_reason                        |STRING  |StateSupervisionPeriodAdmissionReason|
|start_date                              |DATE    |                                     |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=supervision_start_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=supervision_start_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[Missouri](../../states/missouri.md)        |                            240|triggered by code changes|                |
|[North Dakota](../../states/north_dakota.md)|                            240|triggered by code changes|                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                            240|triggered by code changes|                |
|[Tennessee](../../states/tennessee.md)      |                             36|daily                    |                |
|[Tennessee](../../states/tennessee.md)      |                            240|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_start_metrics --show_downstream_dependencies True```

