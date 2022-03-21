## justice_counts_corrections.supervision_starts_probation_aggregated_monthly
SUPERVISION_STARTS_PROBATION aggregated to date partitions

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=justice_counts_corrections&t=supervision_starts_probation_aggregated_monthly)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=justice_counts_corrections&t=supervision_starts_probation_aggregated_monthly)
<br/>

#### Dependency Trees

##### Parentage
[justice_counts_corrections.supervision_starts_probation_aggregated_monthly](../justice_counts_corrections/supervision_starts_probation_aggregated_monthly.md) <br/>
|--[justice_counts_corrections.supervision_starts_probation_monthly_partitions](../justice_counts_corrections/supervision_starts_probation_monthly_partitions.md) <br/>
|----[justice_counts_corrections.supervision_starts_probation_dropped_state](../justice_counts_corrections/supervision_starts_probation_dropped_state.md) <br/>
|------[justice_counts_corrections.supervision_starts_probation_fetch](../justice_counts_corrections/supervision_starts_probation_fetch.md) <br/>
|--------[justice_counts.report_table_instance](../justice_counts/report_table_instance.md) <br/>
|--------[justice_counts.report_table_definition](../justice_counts/report_table_definition.md) <br/>
|--------[justice_counts.cell](../justice_counts/cell.md) <br/>
|----[justice_counts.report_table_instance](../justice_counts/report_table_instance.md) <br/>
|--[justice_counts.report_table_instance](../justice_counts/report_table_instance.md) <br/>
|--[justice_counts.report_table_definition](../justice_counts/report_table_definition.md) <br/>
|--[justice_counts.report](../justice_counts/report.md) <br/>


##### Descendants
[justice_counts_corrections.supervision_starts_probation_aggregated_monthly](../justice_counts_corrections/supervision_starts_probation_aggregated_monthly.md) <br/>
|--[justice_counts_corrections.supervision_starts_probation_monthly_compared](../justice_counts_corrections/supervision_starts_probation_monthly_compared.md) <br/>
|----[justice_counts_corrections.supervision_starts_probation_monthly_with_dimensions](../justice_counts_corrections/supervision_starts_probation_monthly_with_dimensions.md) <br/>
|------[justice_counts_corrections.supervision_starts_probation_output_monthly](../justice_counts_corrections/supervision_starts_probation_output_monthly.md) <br/>
|--------[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|----------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|------------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>

