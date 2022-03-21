## dashboard_views.revocations_matrix_supervision_location_ids_to_names
 Mapping of supervision locations to view names only for locations present in revocations matrix data.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=revocations_matrix_supervision_location_ids_to_names)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=revocations_matrix_supervision_location_ids_to_names)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.revocations_matrix_supervision_location_ids_to_names](../dashboard_views/revocations_matrix_supervision_location_ids_to_names.md) <br/>
|--[shared_metric_views.supervision_matrix_by_person](../shared_metric_views/supervision_matrix_by_person.md) <br/>
|----[shared_metric_views.event_based_commitments_from_supervision_for_matrix](../shared_metric_views/event_based_commitments_from_supervision_for_matrix.md) <br/>
|------[reference_views.agent_external_id_to_full_name](../reference_views/agent_external_id_to_full_name.md) <br/>
|--------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|----------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>
|----[reference_views.agent_external_id_to_full_name](../reference_views/agent_external_id_to_full_name.md) <br/>
|------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|--------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
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


##### Descendants
This view has no child dependencies.
