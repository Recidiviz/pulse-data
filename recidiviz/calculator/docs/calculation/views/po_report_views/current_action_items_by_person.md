## po_report_views.current_action_items_by_person

Client conditions that a PO could take action on as of the current day. 
Unlike retrospective report data, this should be as up to date as possible.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=po_report_views&t=current_action_items_by_person)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=po_report_views&t=current_action_items_by_person)
<br/>

#### Dependency Trees

##### Parentage
This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id po_report_views --view_id current_action_items_by_person --show_downstream_dependencies False```

##### Descendants
[po_report_views.current_action_items_by_person](../po_report_views/current_action_items_by_person.md) <br/>
|--[po_report_views.po_monthly_report_data](../po_report_views/po_monthly_report_data.md) <br/>
|----[validation_views.po_report_avgs_per_district_state](../validation_views/po_report_avgs_per_district_state.md) <br/>
|----[validation_views.po_report_clients](../validation_views/po_report_clients.md) <br/>
|----[validation_views.po_report_distinct_by_officer_month](../validation_views/po_report_distinct_by_officer_month.md) <br/>
|----[validation_views.po_report_missing_fields](../validation_views/po_report_missing_fields.md) <br/>
|----[validation_views.po_report_missing_fields_errors](../validation_views/po_report_missing_fields_errors.md) <br/>

