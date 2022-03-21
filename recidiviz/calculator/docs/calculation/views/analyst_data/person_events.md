## analyst_data.person_events
View concatenating client (person) events in a common format

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=analyst_data&t=person_events)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=analyst_data&t=person_events)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id analyst_data --view_id person_events --show_downstream_dependencies False```

##### Descendants
[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|--[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>

