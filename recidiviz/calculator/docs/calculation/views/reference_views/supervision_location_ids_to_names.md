## reference_views.supervision_location_ids_to_names
Reference table for supervision location names

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=reference_views&t=supervision_location_ids_to_names)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=reference_views&t=supervision_location_ids_to_names)
<br/>

#### Dependency Trees

##### Parentage
[reference_views.supervision_location_ids_to_names](../reference_views/supervision_location_ids_to_names.md) <br/>
|--us_pa_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_location_ids_latest ([Raw Data Doc](../../../ingest/us_pa/raw_data/RECIDIVIZ_REFERENCE_supervision_location_ids.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) <br/>
|--us_nd_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/RECIDIVIZ_REFERENCE_supervision_district_id_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) <br/>
|--us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_region_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_region.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) <br/>
|--us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_name_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) <br/>
|--us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK034_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRDTA_TAK034.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) <br/>
|--static_reference_tables.us_me_cis_908_ccs_location ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_me_cis_908_ccs_location)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_me_cis_908_ccs_location)) <br/>
|--external_reference.us_tn_supervision_facility_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_tn_supervision_facility_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_tn_supervision_facility_names)) <br/>
|--external_reference.us_id_supervision_unit_to_district_map ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_supervision_unit_to_district_map)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_supervision_unit_to_district_map)) <br/>
|--external_reference.us_id_supervision_district_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_supervision_district_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_supervision_district_names)) <br/>


##### Descendants
[reference_views.supervision_location_ids_to_names](../reference_views/supervision_location_ids_to_names.md) <br/>
|--[analyst_data.projected_discharges](../analyst_data/projected_discharges.md) <br/>
|----[analyst_data.us_pa_raw_required_treatment](../analyst_data/us_pa_raw_required_treatment.md) <br/>
|----[linestaff_data_validation.overdue_discharges](../linestaff_data_validation/overdue_discharges.md) <br/>
|------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|----[overdue_discharge_alert.overdue_discharge_alert_data](../overdue_discharge_alert/overdue_discharge_alert_data.md) <br/>
|----[shared_metric_views.overdue_discharge_alert_exclusions](../shared_metric_views/overdue_discharge_alert_exclusions.md) <br/>
|------[overdue_discharge_alert.overdue_discharge_alert_data](../overdue_discharge_alert/overdue_discharge_alert_data.md) <br/>
|--[analyst_data.us_pa_raw_required_treatment](../analyst_data/us_pa_raw_required_treatment.md) <br/>
|--[dashboard_views.pathways_supervision_location_name_map](../dashboard_views/pathways_supervision_location_name_map.md) <br/>
|----[dashboard_views.pathways_supervision_dimension_combinations](../dashboard_views/pathways_supervision_dimension_combinations.md) <br/>
|------[dashboard_views.liberty_to_prison_count_by_month](../dashboard_views/liberty_to_prison_count_by_month.md) <br/>
|------[dashboard_views.supervision_population_time_series](../dashboard_views/supervision_population_time_series.md) <br/>
|------[dashboard_views.supervision_to_liberty_count_by_month](../dashboard_views/supervision_to_liberty_count_by_month.md) <br/>
|------[dashboard_views.supervision_to_prison_count_by_month](../dashboard_views/supervision_to_prison_count_by_month.md) <br/>
|----[dashboard_views.supervision_population_snapshot_by_dimension](../dashboard_views/supervision_population_snapshot_by_dimension.md) <br/>
|----[dashboard_views.supervision_population_time_series](../dashboard_views/supervision_population_time_series.md) <br/>
|----[dashboard_views.supervision_to_liberty_count_by_month](../dashboard_views/supervision_to_liberty_count_by_month.md) <br/>
|----[dashboard_views.supervision_to_liberty_population_snapshot_by_dimension](../dashboard_views/supervision_to_liberty_population_snapshot_by_dimension.md) <br/>
|------[validation_views.supervision_to_liberty_population_snapshot_by_dimension_internal_consistency](../validation_views/supervision_to_liberty_population_snapshot_by_dimension_internal_consistency.md) <br/>
|----[dashboard_views.supervision_to_prison_count_by_month](../dashboard_views/supervision_to_prison_count_by_month.md) <br/>
|----[dashboard_views.supervision_to_prison_population_snapshot_by_dimension](../dashboard_views/supervision_to_prison_population_snapshot_by_dimension.md) <br/>
|----[dashboard_views.supervision_to_prison_population_snapshot_by_officer](../dashboard_views/supervision_to_prison_population_snapshot_by_officer.md) <br/>
|--[dashboard_views.revocations_matrix_supervision_location_ids_to_names](../dashboard_views/revocations_matrix_supervision_location_ids_to_names.md) <br/>
|--[vitals_report_views.supervision_officers_and_districts](../vitals_report_views/supervision_officers_and_districts.md) <br/>
|----[vitals_report_views.supervision_population_by_po_by_day](../vitals_report_views/supervision_population_by_po_by_day.md) <br/>
|------[vitals_report_views.overdue_lsir_by_po_by_day](../vitals_report_views/overdue_lsir_by_po_by_day.md) <br/>
|--------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|--------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|------[vitals_report_views.supervision_downgrade_opportunities_by_po_by_day](../vitals_report_views/supervision_downgrade_opportunities_by_po_by_day.md) <br/>
|--------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|--------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|------[vitals_report_views.supervision_population_due_for_release_by_po_by_day](../vitals_report_views/supervision_population_due_for_release_by_po_by_day.md) <br/>
|--------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|--------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|------[vitals_report_views.timely_contact_by_po_by_day](../vitals_report_views/timely_contact_by_po_by_day.md) <br/>
|--------[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|--------[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>
|--[vitals_report_views.timely_contact_by_po_by_day](../vitals_report_views/timely_contact_by_po_by_day.md) <br/>
|----[dashboard_views.vitals_summaries](../dashboard_views/vitals_summaries.md) <br/>
|----[dashboard_views.vitals_time_series](../dashboard_views/vitals_time_series.md) <br/>

