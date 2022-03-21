# LANTERN DASHBOARD
Lantern is a dashboard for leadership and supervisors to understand the patterns and behaviors associated with revocations. Users can see over-time revocation trends, the violation history leading up to revocation, demographic and geographic breakdowns of revocations, and individual lists of people who were revoked.
## SHIPPED STATES
  - [Missouri](../../states/missouri.md)
  - [Pennsylvania](../../states/pennsylvania.md)

## STATES IN DEVELOPMENT
  N/A

## VIEWS

#### dashboard_views
  - [revocations_matrix_cells](../../views/dashboard_views/revocations_matrix_cells.md) <br/>
  - [revocations_matrix_distribution_by_district](../../views/dashboard_views/revocations_matrix_distribution_by_district.md) <br/>
  - [revocations_matrix_distribution_by_gender](../../views/dashboard_views/revocations_matrix_distribution_by_gender.md) <br/>
  - [revocations_matrix_distribution_by_officer](../../views/dashboard_views/revocations_matrix_distribution_by_officer.md) <br/>
  - [revocations_matrix_distribution_by_race](../../views/dashboard_views/revocations_matrix_distribution_by_race.md) <br/>
  - [revocations_matrix_distribution_by_risk_level](../../views/dashboard_views/revocations_matrix_distribution_by_risk_level.md) <br/>
  - [revocations_matrix_distribution_by_violation](../../views/dashboard_views/revocations_matrix_distribution_by_violation.md) <br/>
  - [revocations_matrix_events_by_month](../../views/dashboard_views/revocations_matrix_events_by_month.md) <br/>
  - [revocations_matrix_filtered_caseload](../../views/dashboard_views/revocations_matrix_filtered_caseload.md) <br/>
  - [revocations_matrix_supervision_location_ids_to_names](../../views/dashboard_views/revocations_matrix_supervision_location_ids_to_names.md) <br/>
  - [state_gender_population](../../views/dashboard_views/state_gender_population.md) <br/>
  - [state_race_ethnicity_population](../../views/dashboard_views/state_race_ethnicity_population.md) <br/>

#### reference_views
  - [agent_external_id_to_full_name](../../views/reference_views/agent_external_id_to_full_name.md) <br/>
  - [augmented_agent_info](../../views/reference_views/augmented_agent_info.md) <br/>
  - [supervision_location_ids_to_names](../../views/reference_views/supervision_location_ids_to_names.md) <br/>

#### shared_metric_views
  - [admission_types_per_state_for_matrix](../../views/shared_metric_views/admission_types_per_state_for_matrix.md) <br/>
  - [event_based_commitments_from_supervision_for_matrix](../../views/shared_metric_views/event_based_commitments_from_supervision_for_matrix.md) <br/>
  - [revocations_matrix_by_person](../../views/shared_metric_views/revocations_matrix_by_person.md) <br/>
  - [supervision_matrix_by_person](../../views/shared_metric_views/supervision_matrix_by_person.md) <br/>
  - [supervision_termination_matrix_by_person](../../views/shared_metric_views/supervision_termination_matrix_by_person.md) <br/>

#### us_mo_raw_data_up_to_date_views
  - LBAKRDTA_TAK034_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRDTA_TAK034.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) <br/>
  - RECIDIVIZ_REFERENCE_supervision_district_to_name_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) <br/>
  - RECIDIVIZ_REFERENCE_supervision_district_to_region_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_region.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) <br/>

#### us_nd_raw_data_up_to_date_views
  - RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/RECIDIVIZ_REFERENCE_supervision_district_id_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) <br/>

#### us_pa_raw_data_up_to_date_views
  - RECIDIVIZ_REFERENCE_supervision_location_ids_latest ([Raw Data Doc](../../../ingest/us_pa/raw_data/RECIDIVIZ_REFERENCE_supervision_location_ids.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) <br/>

## SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

#### external_reference
_Stores data gathered from external sources. CSV versions of tables are committed to our codebase, and updates to tables are fully managed by Terraform._
  - us_id_supervision_district_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_supervision_district_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_supervision_district_names)) <br/>
  - us_id_supervision_unit_to_district_map ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_supervision_unit_to_district_map)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_supervision_unit_to_district_map)) <br/>
  - us_tn_supervision_facility_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_tn_supervision_facility_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_tn_supervision_facility_names)) <br/>

#### state
_Ingested state data. This dataset is a copy of the state postgres database._
  - state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>

#### static_reference_tables
_Reference tables used by various views in BigQuery. May need to be updated manually for new states._
  - state_gender_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_gender_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_gender_population_counts)) <br/>
  - state_race_ethnicity_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_race_ethnicity_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_race_ethnicity_population_counts)) <br/>
  - us_me_cis_908_ccs_location ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_me_cis_908_ccs_location)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_me_cis_908_ccs_location)) <br/>

## METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

|                                                         **Metric**                                                          |      **US_ID**      |      **US_ME**      |      **US_MO**      |      **US_ND**      |      **US_PA**      |      **US_TN**      |
|-----------------------------------------------------------------------------------------------------------------------------|---------------------|---------------------|---------------------|---------------------|---------------------|---------------------|
|[INCARCERATION_COMMITMENT_FROM_SUPERVISION](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md)|36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[SUPERVISION_POPULATION](../../metrics/supervision/supervision_population_metrics.md)                                        |36 months 240 months |                     |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
|[SUPERVISION_TERMINATION](../../metrics/supervision/supervision_termination_metrics.md)                                      |36 months 240 months |                     |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
