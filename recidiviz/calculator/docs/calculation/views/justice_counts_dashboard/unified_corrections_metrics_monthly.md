## justice_counts_dashboard.unified_corrections_metrics_monthly
Unified view of all calculated corrections metrics by month

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=justice_counts_dashboard&t=unified_corrections_metrics_monthly)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=justice_counts_dashboard&t=unified_corrections_metrics_monthly)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id justice_counts_dashboard --view_id unified_corrections_metrics_monthly --show_downstream_dependencies False```

##### Descendants
[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|--[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|----[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>

