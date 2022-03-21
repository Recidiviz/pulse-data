## ViolationWithResponseMetric

The `ViolationWithResponseMetric` stores information about when a report has been submitted about a person on supervision violating one or more conditions of their supervision. This metric tracks the date of the earliest response to a violation incident for each type of violation associated with the incident.

With this metric, we can answer questions like:

- Has Person X ever been written up for a substance use violation?
- How has the number of reported technical violations changed over time?
- Does the frequency of reported municipal violations vary by the age of the person on supervision?

This metric is derived from the `StateSupervisionViolation` and `StateSupervisionViolationResponse` entities. The `StateSupervisionViolation` entities store information about the violation incident that occurred, and the `StateSupervisionViolationResponse` entities store information about how agents within the corrections system responded to the violation. There is one `ViolationWithResponseMetric` created for each of the violation types present on each `StateSupervisionViolation`, through the `supervision_violation_types` relationship to the `StateSupervisionViolationTypeEntry` entities. The date associated with each `ViolationWithResponseMetric` is the `response_date` of the earliest `StateSupervisionViolationResponse` associated with the violation. This is generally the date that the first violation report or citation was submitted describing the violating behavior. We track violations by the date of the first report since the date a report was submitted is required in most state databases, whereas the date of the violating behavior is not consistently reported. A violation must have a report with a set `response_date` to be included in the calculations.

Some states have defined state-specific violation type subtypes. For example, in Pennsylvania the `TECHNICAL` violations are broken down into categories of `HIGH_TECH`, `MED_TECH` and `LOW_TECH`. In these states, one `ViolationWithResponseMetric` is produced for each violation type subtype present on the `StateSupervisionViolation`.

If a parole officer submitted a violation report on 2018-09-07 describing violating behavior that occurred on 2018-09-01, where the behavior included both `TECHNICAL` and `MUNICIPAL` violations, then there will be two `ViolationWithResponseMetrics` produced for the date `2018-09-07`, one with `violation_type=TECHNICAL` and the other with `violation_type=MUNICIPAL`. Both metrics will also record the date of the violation, with `violation_date=2018-09-01`. 

If a probation officer in US_PA submitted a violation report on 2019-11-19 describing violating behavior that fell into both the `LOW_TECH` and `MED_TECH` categories of violation subtypes for the state, then there will be two `ViolationWithResponseMetrics` produced for the date `2019-11-19` with `violation_type=TECHNICAL`. One will have `violation_type_subtype=LOW_TECH` and the other will have `violation_type_subtype=MED_TECH`. Neither metrics will have a set `violation_date` field, since the date of the violating behavior was not recorded on the violation report.


#### Metric attributes
Attributes specific to the `ViolationWithResponseMetric`:

|                **Attribute Name**                |**Type**|             **Enum Class**              |
|--------------------------------------------------|--------|-----------------------------------------|
|year                                              |INTEGER |                                         |
|month                                             |INTEGER |                                         |
|supervision_violation_id                          |INTEGER |                                         |
|violation_type                                    |STRING  |StateSupervisionViolationType            |
|violation_type_subtype                            |STRING  |                                         |
|is_most_severe_violation_type                     |BOOLEAN |                                         |
|violation_date                                    |DATE    |                                         |
|is_violent                                        |BOOLEAN |                                         |
|is_sex_offense                                    |BOOLEAN |                                         |
|most_severe_response_decision                     |STRING  |StateSupervisionViolationResponseDecision|
|is_most_severe_violation_type_of_all_violations   |BOOLEAN |                                         |
|is_most_severe_response_decision_of_all_violations|BOOLEAN |                                         |
|response_date                                     |DATE    |                                         |


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=violation_with_response_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=violation_with_response_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[Missouri](../../states/missouri.md)        |                             36|daily                    |                |
|[Missouri](../../states/missouri.md)        |                            240|triggered by code changes|                |
|[Pennsylvania](../../states/pennsylvania.md)|                             36|daily                    |                |
|[Pennsylvania](../../states/pennsylvania.md)|                            240|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_violation_with_response_metrics --show_downstream_dependencies True```

