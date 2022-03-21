## IncarcerationPopulationMetric

The `IncarcerationPopulationMetric` stores information about a single day that an individual spent incarcerated. This metric tracks each day on which an individual was counted in the state’s incarcerated population, and includes information related to the stay in a facility.

With this metric, we can answer questions like:

- How has the population of a DOC facility changed over time?
- How many people are being held in a state prison for a parole board hold today?
- What proportion of individuals incarcerated in a state in 2010 were women?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility. All population metrics are end date exclusive, meaning a person is not counted in a facility’s population on the day that they are released from that facility. The population metrics are start date inclusive, meaning that a person is counted in a facility’s population on the date that they are admitted to the facility.


#### Metric attributes
Attributes specific to the `IncarcerationPopulationMetric`:

|             **Attribute Name**             |**Type**|            **Enum Class**             |
|--------------------------------------------|--------|---------------------------------------|
|secondary_person_external_id                |STRING  |                                       |
|year                                        |INTEGER |                                       |
|month                                       |INTEGER |                                       |
|included_in_state_population                |BOOLEAN |                                       |
|facility                                    |STRING  |                                       |
|county_of_residence                         |STRING  |                                       |
|date_of_stay                                |DATE    |                                       |
|admission_reason                            |STRING  |StateIncarcerationPeriodAdmissionReason|
|admission_reason_raw_text                   |STRING  |                                       |
|commitment_from_supervision_supervision_type|STRING  |StateSupervisionPeriodSupervisionType  |
|judicial_district_code                      |STRING  |                                       |
|specialized_purpose_for_incarceration       |STRING  |StateSpecializedPurposeForIncarceration|
|custodial_authority                         |STRING  |StateCustodialAuthority                |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=incarceration_population_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=incarceration_population_metrics)
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

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_population_metrics_included_in_state_population --show_downstream_dependencies True```
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_population_metrics_not_included_in_state_population --show_downstream_dependencies True```

