## ReincarcerationRecidivismCountMetric

The `ReincarcerationRecidivismCountMetric` stores information about when a person who has previously been released from prison is admitted to prison again. This metric tracks the date of the reincarceration, as well as other details about the reincarceration.

The term “recidivism” is defined in many ways throughout the criminal justice system. This metric explicitly calculates instances of reincarceration recidivism, which is defined as a person being incarcerated again after having previously been released from incarceration. That is, each time a single person is released from prison — to supervision and/or to full liberty — and is later reincarcerated in prison, that is counted as an instance of recidivism.

With this metric, we can answer questions like:

- How many people were admitted to prison in 2019 that had previously been incarcerated?
- For all of the people with reincarceration admissions in 2020, how many days, on average, had they spent at liberty since the time they were last released from prison?
- Does the number of reincarceration admissions in the state vary by the county in which the people lived while they were at liberty?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility.

These calculations work by looking at all releases from prison, where the prison stay qualifies as a formal stay in incarceration, and the release is either to supervision or to liberty. We then look for admissions back into formal incarceration that followed these official releases from formal incarceration, which are reincarceration admissions. Parole board holds or other forms of temporary custody are not included as formal stays in incarceration, even if they occur in a state prison facility. Admissions to parole board holds or other forms of temporary custody are not considered reincarceration admissions. Admissions for parole revocations that may follow time spent in a parole board hold, however, do qualify has reincarceration recidivism. Admissions for probation revocations can also be classified as reincarceration recidivism if the person was previously incarcerated. Sanction admissions from supervision for treatment or shock incarceration can also be considered reincarceration recidivism if the person was previously incarceration.

Say a person was released to liberty on 2008-01-03 from a state prison, where they had served the entirety of a new sentence. Then 10 years later, on 2018-01-26, they are admitted to prison for a new sentence. In this case, there will be one `ReincarcerationRecidivismCountMetric` produced with a `reincarceration_date` of 2018-01-26, the date of the reincarceration admission to prison.

Say a person was released to parole on 2015-02-18 from a state prison, where they were being held to serve a new sentence. Then, 8 months later on 2015-10-16, they are brought into a parole board hold in response to a parole violation. They are held in the parole board hold for 20 days while they wait for a decision from the parole board. On 2015-11-05 they are formally revoked by the parole board, and are officially admitted to the prison for the parole revocation. In this case, there will be one `ReincarcerationRecidivismCountMetric` produced with a `reincarceration_date` of 2015-11-05, the date of the formal parole revocation admission.


#### Metric attributes
Attributes specific to the `ReincarcerationRecidivismCountMetric`:

| **Attribute Name** |**Type**|**Enum Class**|
|--------------------|--------|--------------|
|stay_length_bucket  |STRING  |              |
|release_facility    |STRING  |              |
|county_of_residence |STRING  |              |
|year                |INTEGER |              |
|month               |INTEGER |              |
|days_at_liberty     |INTEGER |              |
|reincarceration_date|DATE    |              |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=recidivism_count_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=recidivism_count_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|-------------------------------|-------------------------|----------------|
|[North Dakota](../../states/north_dakota.md)|N/A                            |daily                    |                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_recidivism_count_metrics --show_downstream_dependencies True```

