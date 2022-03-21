## vitals_report_views.supervision_officers_and_districts

Mapping of supervision officers and their districts


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vitals_report_views&t=supervision_officers_and_districts)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vitals_report_views&t=supervision_officers_and_districts)
<br/>

#### Dependency Trees

##### Parentage
[vitals_report_views.supervision_officers_and_districts](../vitals_report_views/supervision_officers_and_districts.md) <br/>
|--static_reference_tables.us_id_roster ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_id_roster)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_id_roster)) <br/>
|--[reference_views.supervision_location_ids_to_names](../reference_views/supervision_location_ids_to_names.md) <br/>
|----us_pa_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_location_ids_latest ([Raw Data Doc](../../../ingest/us_pa/raw_data/RECIDIVIZ_REFERENCE_supervision_location_ids.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) <br/>
|----us_nd_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/RECIDIVIZ_REFERENCE_supervision_district_id_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) <br/>
|----us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_region_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_region.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) <br/>
|----us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_name_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) <br/>
|----us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK034_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRDTA_TAK034.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) <br/>
|----static_reference_tables.us_me_cis_908_ccs_location ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_me_cis_908_ccs_location)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_me_cis_908_ccs_location)) <br/>
|----external_reference.us_tn_supervision_facility_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_tn_supervision_facility_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_tn_supervision_facility_names)) <br/>
|----external_reference.us_id_supervision_unit_to_district_map ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_supervision_unit_to_district_map)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_supervision_unit_to_district_map)) <br/>
|----external_reference.us_id_supervision_district_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_supervision_district_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_supervision_district_names)) <br/>
|--[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>


##### Descendants
[vitals_report_views.supervision_officers_and_districts](../vitals_report_views/supervision_officers_and_districts.md) <br/>
|--[vitals_report_views.supervision_population_by_po_by_day](../vitals_report_views/supervision_population_by_po_by_day.md) <br/>
|----[vitals_report_views.overdue_lsir_by_po_by_day](../vitals_report_views/overdue_lsir_by_po_by_day.md) <br/>
|------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|----[vitals_report_views.supervision_downgrade_opportunities_by_po_by_day](../vitals_report_views/supervision_downgrade_opportunities_by_po_by_day.md) <br/>
|------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|----[vitals_report_views.supervision_population_due_for_release_by_po_by_day](../vitals_report_views/supervision_population_due_for_release_by_po_by_day.md) <br/>
|------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|----[vitals_report_views.timely_contact_by_po_by_day](../vitals_report_views/timely_contact_by_po_by_day.md) <br/>
|------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>

