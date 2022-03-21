## experiments.person_outcomes
Calculates outcome metrics for persons (clients) assigned a variant in the person assignments table. All metrics are specific to person-experiment-variant, so may have multiple observations per person and per person-experiment.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=experiments&t=person_outcomes)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=experiments&t=person_outcomes)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id experiments --view_id person_outcomes --show_downstream_dependencies False```

##### Descendants
This view has no child dependencies.
