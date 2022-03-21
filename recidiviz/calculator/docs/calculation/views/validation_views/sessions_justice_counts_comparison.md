## validation_views.sessions_justice_counts_comparison

Comparison of justice counts and sessions data on prison, parole, and probation populations (project agnostic)


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=sessions_justice_counts_comparison)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=sessions_justice_counts_comparison)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id validation_views --view_id sessions_justice_counts_comparison --show_downstream_dependencies False```

##### Descendants
[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>

