## po_report_views.po_monthly_report_data

 Monthly data regarding an officer's success in discharging people from supervision, recommending early discharge
 from supervision, and keeping cases in compliance with state standards.
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=po_report_views&t=po_monthly_report_data)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=po_report_views&t=po_monthly_report_data)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id po_report_views --view_id po_monthly_report_data --show_downstream_dependencies False```

##### Descendants
[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|--[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|--[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|--[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|--[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|--[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>

