## IncarcerationReleaseMetric

The `IncarcerationReleaseMetric` stores information about a release from incarceration. This metric tracks each time that an individual was released from an incarceration facility, and includes information related to the release.

With this metric, we can answer questions like:

- What was the most common release reason for all individuals released from prison in 2019?
- For all individuals released from Facility X in 2015, what was the average number of days each person spent incarcerated prior to their release?
- How did the number of releases per month change after the state implemented a new release policy?

This metric is derived from the `StateIncarcerationPeriod` entities, which store information about periods of time that an individual was in an incarceration facility.  

If a person was admitted to Facility X on 2021-01-01, was transferred out of Facility X and into Facility Z on 2021-03-31, and then was released from Facility Z on 2021-06-09, then there will be a single `IncarcerationReleaseMetric` for this person on 2021-06-09 from Facility Z. Transfer releases are not included in this metric.


#### Metric attributes
Attributes specific to the `IncarcerationReleaseMetric`:

|             **Attribute Name**             |**Type**|            **Enum Class**             |
|--------------------------------------------|--------|---------------------------------------|
|secondary_person_external_id                |STRING  |                                       |
|year                                        |INTEGER |                                       |
|month                                       |INTEGER |                                       |
|included_in_state_population                |BOOLEAN |                                       |
|facility                                    |STRING  |                                       |
|county_of_residence                         |STRING  |                                       |
|release_date                                |DATE    |                                       |
|release_reason                              |STRING  |StateIncarcerationPeriodReleaseReason  |
|release_reason_raw_text                     |STRING  |                                       |
|purpose_for_incarceration                   |STRING  |StateSpecializedPurposeForIncarceration|
|supervision_type_at_release                 |STRING  |StateSupervisionPeriodSupervisionType  |
|admission_reason                            |STRING  |StateIncarcerationPeriodAdmissionReason|
|total_days_incarcerated                     |INTEGER |                                       |
|commitment_from_supervision_supervision_type|STRING  |StateSupervisionPeriodSupervisionType  |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=incarceration_release_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=incarceration_release_metrics)
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

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_release_metrics_included_in_state_population --show_downstream_dependencies True```
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_release_metrics_not_included_in_state_population --show_downstream_dependencies True```

