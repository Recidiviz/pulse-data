## dataflow_metrics_materialized.most_recent_supervision_start_metrics
supervision_start_metrics for the most recent job run

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics_materialized&t=most_recent_supervision_start_metrics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics_materialized&t=most_recent_supervision_start_metrics)
<br/>

#### Dependency Trees

##### Parentage
[dataflow_metrics_materialized.most_recent_supervision_start_metrics](../dataflow_metrics_materialized/most_recent_supervision_start_metrics.md) <br/>
|--[dataflow_metrics.supervision_start_metrics](../../metrics/supervision/supervision_start_metrics.md) <br/>


##### Descendants
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_supervision_start_metrics --show_downstream_dependencies True```
