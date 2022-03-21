## public_dashboard_views.active_program_participation_by_region
Active program participation counts by the region of the program location.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=active_program_participation_by_region)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=active_program_participation_by_region)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.active_program_participation_by_region](../public_dashboard_views/active_program_participation_by_region.md) <br/>
|--us_nd_raw_data_up_to_date_views.docstars_REF_PROVIDER_LOCATION_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/docstars_REF_PROVIDER_LOCATION.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=docstars_REF_PROVIDER_LOCATION_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=docstars_REF_PROVIDER_LOCATION_latest)) <br/>
|--[dataflow_metrics_materialized.most_recent_single_day_program_participation_metrics](../dataflow_metrics_materialized/most_recent_single_day_program_participation_metrics.md) <br/>
|----[dataflow_metrics_materialized.most_recent_program_participation_metrics](../dataflow_metrics_materialized/most_recent_program_participation_metrics.md) <br/>
|------[dataflow_metrics.program_participation_metrics](../../metrics/program/program_participation_metrics.md) <br/>


##### Descendants
[public_dashboard_views.active_program_participation_by_region](../public_dashboard_views/active_program_participation_by_region.md) <br/>
|--[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|--[validation_views.active_program_participation_by_region_internal_consistency](../validation_views/active_program_participation_by_region_internal_consistency.md) <br/>
|--[validation_views.active_program_participation_by_region_internal_consistency_errors](../validation_views/active_program_participation_by_region_internal_consistency_errors.md) <br/>

