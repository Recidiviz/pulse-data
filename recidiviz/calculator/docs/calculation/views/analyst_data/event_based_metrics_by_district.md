## analyst_data.event_based_metrics_by_district
Reports metrics counting events at district-level at various time scales on a daily cadence

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=analyst_data&t=event_based_metrics_by_district)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=analyst_data&t=event_based_metrics_by_district)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id analyst_data --view_id event_based_metrics_by_district --show_downstream_dependencies False```

##### Descendants
This view has no child dependencies.
