## sessions.admission_start_reason_dedup_priority

This view defines a prioritized ranking for supervision start reasons and incarceration admission reasons. This view is
ultimately used to deduplicate incarceration admissions and supervision starts so that there is only one event per person
per day. Prioritization and deduplication is done within incarceration and within supervision meaning that a person
could in theory have both a supervision start and incarceration admission on the same day. Deduplication across incarceration
and supervision is handled based on a join condition to deduplicated population metrics


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sessions&t=admission_start_reason_dedup_priority)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sessions&t=admission_start_reason_dedup_priority)
<br/>

#### Dependency Trees

##### Parentage
This view has no parent dependencies.

##### Descendants
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id sessions --view_id admission_start_reason_dedup_priority --show_downstream_dependencies True```
