## validation_views.incarceration_population_by_state_by_date_justice_counts_comparison_errors
 Comparison of justice counts population with internal incarceration population counts by month 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=incarceration_population_by_state_by_date_justice_counts_comparison_errors)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=incarceration_population_by_state_by_date_justice_counts_comparison_errors)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.incarceration_population_by_state_by_date_justice_counts_comparison_errors](../validation_views/incarceration_population_by_state_by_date_justice_counts_comparison_errors.md) <br/>
|--[justice_counts_corrections.population_prison_output_monthly](../justice_counts_corrections/population_prison_output_monthly.md) <br/>
|----[justice_counts_corrections.population_prison_monthly_with_dimensions](../justice_counts_corrections/population_prison_monthly_with_dimensions.md) <br/>
|------[justice_counts_corrections.population_prison_monthly_compared](../justice_counts_corrections/population_prison_monthly_compared.md) <br/>
|--------[justice_counts_corrections.population_prison_aggregated_monthly](../justice_counts_corrections/population_prison_aggregated_monthly.md) <br/>
|----------[justice_counts_corrections.population_prison_monthly_partitions](../justice_counts_corrections/population_prison_monthly_partitions.md) <br/>
|------------[justice_counts_corrections.population_prison_dropped_state](../justice_counts_corrections/population_prison_dropped_state.md) <br/>
|--------------[justice_counts_corrections.population_prison_fetch](../justice_counts_corrections/population_prison_fetch.md) <br/>
|----------------[justice_counts.report_table_instance](../justice_counts/report_table_instance.md) <br/>
|----------------[justice_counts.report_table_definition](../justice_counts/report_table_definition.md) <br/>
|----------------[justice_counts.cell](../justice_counts/cell.md) <br/>
|------------[justice_counts.report_table_instance](../justice_counts/report_table_instance.md) <br/>
|----------[justice_counts.report_table_instance](../justice_counts/report_table_instance.md) <br/>
|----------[justice_counts.report_table_definition](../justice_counts/report_table_definition.md) <br/>
|----------[justice_counts.report](../justice_counts/report.md) <br/>
|----[justice_counts.source](../justice_counts/source.md) <br/>
|----[justice_counts.report](../justice_counts/report.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
