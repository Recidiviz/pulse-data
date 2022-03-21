## SupervisionDowngradeMetric

The `SupervisionDowngradeMetric` stores instances of when the supervision level of a person’s supervision is changed to a level that is less restrictive. This metric tracks the day that the downgrade occurred, as well as other information related to the supervision that was downgraded.

With this metric, we can answer questions like:

- How many people were downgraded from a supervision level of `MEDIUM` to `MINIMUM` in 2020?
- How did the number of people downgraded to the `UNSUPERVISED` supervision level change after the state requirements for the downgrade were updated in 2019?
- Were there more supervision downgrades for men or women last month?

This metric is derived from the `supervision_level` values on the `StateSupervisionPeriod` entities, which store information about periods of time that an individual was supervised. The calculations for this metric only identify supervision level downgrades for states where a new supervision period is created when the supervision level changes. If the `supervision_level` value of a period is updated in place when a downgrade occurs, then these calculations will not be able to capture that downgrade in this metric. We also consider supervision level downgrades between two adjacent supervision periods, where the `termination_date` of one period is the same as the `start_date` of the next period. If a person ends a supervision period with a `MAXIMUM` supervision level on 2018-04-12, and then starts a new supervision period with a `MEDIUM` supervision level months later, on 2018-11-17, then this is not counted as a supervision level downgrade.

We are only able to calculate supervision downgrades between two supervision levels that fall within the clear level hierarchy (e.g. `MAXIMUM`, `HIGH`, `MEDIUM`, etc). We are not able to identify if a downgrade occurred if a person is on a supervision level of, say, `DIVERSION`, which doesn’t have an explicit correlation to a degree of restriction for the supervision. 

Say a person started serving a period of probation with a `supervision_level` of `MEDIUM` on 2020-01-01, and then on 2020-08-13 this supervision period is terminated and they start a new supervision period with a `supervision_level` of `MINIMUM`. There would be a `SupervisionDowngradeMetric` for this downgrade of supervision levels with a `date_of_downgrade` of 2020-08-13, a `previous_supervision_level` of `MEDIUM` and a `supervision_level` of `MINIMUM`. 


#### Metric attributes
Attributes specific to the `SupervisionDowngradeMetric`:

|           **Attribute Name**           |**Type**|           **Enum Class**            |
|----------------------------------------|--------|-------------------------------------|
|supervising_district_external_id        |STRING  |                                     |
|level_1_supervision_location_external_id|STRING  |                                     |
|level_2_supervision_location_external_id|STRING  |                                     |
|year                                    |INTEGER |                                     |
|month                                   |INTEGER |                                     |
|supervision_type                        |STRING  |StateSupervisionPeriodSupervisionType|
|case_type                               |STRING  |StateSupervisionCaseType             |
|supervision_level_raw_text              |STRING  |                                     |
|supervising_officer_external_id         |STRING  |                                     |
|judicial_district_code                  |STRING  |                                     |
|custodial_authority                     |STRING  |StateCustodialAuthority              |
|date_of_downgrade                       |DATE    |                                     |
|previous_supervision_level              |STRING  |StateSupervisionLevel                |
|supervision_level                       |STRING  |StateSupervisionLevel                |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=supervision_downgrade_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=supervision_downgrade_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_downgrade_metrics --show_downstream_dependencies True```

