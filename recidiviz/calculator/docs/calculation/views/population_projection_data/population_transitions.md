## population_projection_data.population_transitions
"Historical total population by compartment, outflow compartment,
and compartment duration (months)

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=population_projection_data&t=population_transitions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=population_projection_data&t=population_transitions)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id population_projection_data --view_id population_transitions --show_downstream_dependencies False```

##### Descendants
[population_projection_data.population_transitions](../population_projection_data/population_transitions.md) <br/>
|--[population_projection_data.incarceration_remaining_sentences](../population_projection_data/incarceration_remaining_sentences.md) <br/>
|----[population_projection_data.remaining_sentences](../population_projection_data/remaining_sentences.md) <br/>
|--[population_projection_data.supervision_remaining_sentences](../population_projection_data/supervision_remaining_sentences.md) <br/>
|----[population_projection_data.remaining_sentences](../population_projection_data/remaining_sentences.md) <br/>

