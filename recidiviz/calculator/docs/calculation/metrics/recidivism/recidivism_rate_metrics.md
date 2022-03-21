## ReincarcerationRecidivismRateMetric

The `ReincarcerationRecidivismRateMetric` stores information about whether or not a person is ever readmitted to prison within a certain number of years after being officially released from incarceration. This metric tracks the date that the person was released from incarceration, the calendar year release cohort that the individual was included in, and the number of years after the date of release during which recidivism was measured.

The term “recidivism” is defined in many ways throughout the criminal justice system. This metric explicitly calculates instances of reincarceration recidivism, which is defined as a person being incarcerated again after having previously been released from incarceration. That is, each time a single person is released from prison — to supervision and/or to full liberty — and is later reincarcerated in prison, that is counted as an instance of recidivism.

With this metric, we can answer questions like:

- How many people that were released from prison in 2015 had returned to prison within the next 3 years?
- Over the last 10 years, what was the average rate of recidivism within 1 year of release?
- How does the 5 year recidivism rate differ by gender in this state?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility.

These calculations work by looking at all releases from prison, where the prison stay qualifies as a formal stay in incarceration, and the release is either to supervision or to liberty. For each qualified release, we look for admissions back into formal incarceration following the release that are reincarceration admissions.

Parole board holds or other forms of temporary custody are not included as formal stays in incarceration, even if they occur in a state prison facility. Admissions to parole board holds or other forms of temporary custody are not considered reincarceration admissions. Admissions for parole revocations that may follow time spent in a parole board hold, however, do qualify has reincarceration recidivism. Admissions for probation revocations can also be classified as reincarceration recidivism if the person was previously incarcerated. Sanction admissions from supervision for treatment or shock incarceration can also be considered reincarceration recidivism if the person was previously incarceration.

For each qualified release from prison, we produce `ReincarcerationRecidivismRateMetrics` for up to 10 follow-up-periods, for each integer year since the release. We only produce a `ReincarcerationRecidivismRateMetric` for a follow-up-period if the period has already completed or is in progress. For example, if a person was released on 2018-01-01, and today is 2021–01-01, then the following follow-up-periods have completed:

- `follow_up_period=1`: 2018-01-01 to 2018-12-31
- `follow_up_period=2`: 2018-01-01 to 2019-12-31
- `follow_up_period=3`: 2018-01-01 to 2020-12-31

This follow-up-period has just started:

- `follow_up_period=4`: 2018-01-01 to 2021-12-31

For this release, we will create a `ReincarcerationRecidivismRateMetric` for the follow-up-periods 1 through 4. We will not create metrics for any follow-up-period values 5 or greater, since those periods are not yet in progress.

For each of the relevant follow-up-period windows, we create a `ReincarcerationRecidivismRateMetric` indicating whether the person was reincarcerated within the number of years after the release. If the person was not reincarcerated within the follow-up-period window, then there will be a single `ReincarcerationRecidivismRateMetric` created where `did_recidivate=False`. If a person was reincarcerated within the follow-up-period window, then one `ReincarcerationRecidivismRateMetric` is produced for each unique reincarceration admission within the window. 

Say a person was released to liberty on 2008-01-03 from a state prison, where they had served the entirety of a new sentence. Then, a little more than 6 years later, on 2014-01-26, they are admitted to prison for a new sentence, and they are still incarcerated from this admission today. In this case, there will be 10 `ReincarcerationRecidivismRateMetrics` produced with the following attributes: 

| release_cohort | follow_up_period | did_recidivate |
| -------------- | ---------------- | -------------- |
| 2008           | 1                | False          |
| 2008           | 2                | False          |
| 2008           | 3                | False          |
| 2008           | 4                | False          |
| 2008           | 5                | False          |
| 2008           | 6                | False          |
| 2008           | 7                | True           |
| 2008           | 8                | True           |
| 2008           | 9                | True           |
| 2008           | 10               | True           |

Say a person was released to liberty on 2008-01-03 from a state prison, where they had served the entirety of a new sentence. Then, a little more than 2 years later, on 2010-01-13, they are admitted to prison for a new sentence. They are later released from prison back to liberty on 2016-09-08, and then reincarcerated a second time on 2017-05-13. In this case, if we run the recidivism calculations on 2021-01-01, then the following metrics will be produced:

| release_cohort | follow_up_period | did_recidivate |
| -------------- | ---------------- | -------------- |
| 2008           | 1                | False          |
| 2008           | 2                | False          |
| 2008           | 3                | True           |
| 2008           | 4                | True           |
| 2008           | 5                | True           |
| 2008           | 6                | True           |
| 2008           | 7                | True           |
| 2008           | 8                | True           |
| 2008           | 9                | True           |
| 2008           | 10*              | True           |
| 2008           | 10*              | True           |
| 2016           | 1                | True           |
| 2016           | 2                | True           |
| 2016           | 3                | True           |
| 2016           | 4                | True           |
| 2016           | 5                | True           |

*Note that there are two metrics produced for `release_cohort=2008` and `follow_up_period=10`, since there are two reincarcerations within the 10-year follow-up-period window. 


#### Metric attributes
Attributes specific to the `ReincarcerationRecidivismRateMetric`:

|**Attribute Name** |**Type**|**Enum Class**|
|-------------------|--------|--------------|
|stay_length_bucket |STRING  |              |
|release_facility   |STRING  |              |
|county_of_residence|STRING  |              |
|release_cohort     |INTEGER |              |
|follow_up_period   |INTEGER |              |
|did_recidivate     |BOOLEAN |              |
|release_date       |DATE    |              |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=recidivism_rate_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=recidivism_rate_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|-------------------------------|-------------------------|----------------|
|[North Dakota](../../states/north_dakota.md)|N/A                            |daily                    |                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_recidivism_rate_metrics --show_downstream_dependencies True```

