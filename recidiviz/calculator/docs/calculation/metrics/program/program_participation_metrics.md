## ProgramParticipationMetric

The `ProgramParticipationMetric` stores information about a person participating in rehabilitative programming. This metric tracks each day that a person was actively participating in a given program, and stores information related to that participation.

With this metric, we can answer questions like:

- How many people participated in Program X in the month of April 2020?
- How has the participation in Program Y grown since it was introduced in January 2017?
- Of all of the people currently participating in Program Z in the state, what percent are under the age of 30?

 
This metric is derived from the `StateProgramAssignment` entities, which store information about the assignment of a person to some form of rehabilitative programming -- and their participation in the program -- intended to address specific needs of the person. The calculations for this metric look for `StateProgramAssignment` instances with a `participation_status` of either `IN_PROGRESS` or `DISCHARGED`, and use the `start_date` and `discharge_date` fields to produce a single `ProgramParticipationMetric` for each day that a person was actively participating in the program.

If a person started participating in Program X on April 1, 2020 and were discharged on May 1, 2020, then there will be a `ProgramParticipationMetric` for each day of participation in Program X (30 `ProgramParticipationMetric` outputs in total).

If a person is participating in a program while they are on supervision, then this metric records the supervision type the person was on on the date of the participation. If a person is serving multiple supervisions simultaneously (and has multiple `StateSupervisionPeriod` entities that overlap a participation date) then there will be one `ProgramParticipationMetric` produced for each overlapping supervision period. So, if a person participated in Program Z for a single day, on October 26, 2014, and on that day the person was serving both probation and parole simultaneously (represented by two overlapping `StateSupervisionPeriod` entities), then there will be two `ProgramParticipationMetrics` produced: one with a `supervision_type` of `PAROLE` and one with a `supervision_type` of `PROBATION`.     


#### Metric attributes
Attributes specific to the `ProgramParticipationMetric`:

|  **Attribute Name**   |**Type**|           **Enum Class**            |
|-----------------------|--------|-------------------------------------|
|year                   |INTEGER |                                     |
|month                  |INTEGER |                                     |
|program_id             |STRING  |                                     |
|date_of_participation  |DATE    |                                     |
|is_first_day_in_program|BOOLEAN |                                     |
|program_location_id    |STRING  |                                     |
|supervision_type       |STRING  |StateSupervisionPeriodSupervisionType|


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

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=program_participation_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=program_participation_metrics)
<br/>

#### Calculation Cadences

|                 **State**                  |**Number of Months Calculated**|**Calculation Frequency**|**Staging Only**|
|--------------------------------------------|------------------------------:|-------------------------|----------------|
|[Idaho](../../states/idaho.md)              |                             36|daily                    |                |
|[Idaho](../../states/idaho.md)              |                            240|triggered by code changes|                |
|[North Dakota](../../states/north_dakota.md)|                             36|daily                    |                |
|[North Dakota](../../states/north_dakota.md)|                             60|triggered by code changes|                |


#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_program_participation_metrics --show_downstream_dependencies True```

