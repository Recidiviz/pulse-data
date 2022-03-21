# VITALS
Vitals provides a snapshot of agency performance on a set of key operational metrics, which can be drilled down by geographic region and officer. This helps community supervision leadership and supervisors proactively identify resource constraints as well as opportunities for improvement in both compliance and policy.
## SHIPPED STATES
  - [Idaho](../../states/idaho.md)
  - [North Dakota](../../states/north_dakota.md)

## STATES IN DEVELOPMENT
  N/A

## VIEWS

#### dashboard_views
  - [vitals_summaries](../../views/dashboard_views/vitals_summaries.md) <br/>
  - [vitals_time_series](../../views/dashboard_views/vitals_time_series.md) <br/>

#### reference_views
  - [agent_external_id_to_full_name](../../views/reference_views/agent_external_id_to_full_name.md) <br/>
  - [augmented_agent_info](../../views/reference_views/augmented_agent_info.md) <br/>
  - [supervision_location_ids_to_names](../../views/reference_views/supervision_location_ids_to_names.md) <br/>

#### shared_metric_views
  - [supervision_case_compliance_metrics](../../views/shared_metric_views/supervision_case_compliance_metrics.md) <br/>
  - [supervision_mismatches_by_day](../../views/shared_metric_views/supervision_mismatches_by_day.md) <br/>

#### us_mo_raw_data_up_to_date_views
  - LBAKRDTA_TAK034_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRDTA_TAK034.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK034_latest)) <br/>
  - RECIDIVIZ_REFERENCE_supervision_district_to_name_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_name_latest)) <br/>
  - RECIDIVIZ_REFERENCE_supervision_district_to_region_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/RECIDIVIZ_REFERENCE_supervision_district_to_region.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_to_region_latest)) <br/>

#### us_nd_raw_data_up_to_date_views
  - RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/RECIDIVIZ_REFERENCE_supervision_district_id_to_name.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest)) <br/>

#### us_pa_raw_data_up_to_date_views
  - RECIDIVIZ_REFERENCE_supervision_location_ids_latest ([Raw Data Doc](../../../ingest/us_pa/raw_data/RECIDIVIZ_REFERENCE_supervision_location_ids.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_pa_raw_data_up_to_date_views&t=RECIDIVIZ_REFERENCE_supervision_location_ids_latest)) <br/>

#### vitals_report_views
  - [overdue_lsir_by_po_by_day](../../views/vitals_report_views/overdue_lsir_by_po_by_day.md) <br/>
  - [supervision_downgrade_opportunities_by_po_by_day](../../views/vitals_report_views/supervision_downgrade_opportunities_by_po_by_day.md) <br/>
  - [supervision_officers_and_districts](../../views/vitals_report_views/supervision_officers_and_districts.md) <br/>
  - [supervision_population_by_po_by_day](../../views/vitals_report_views/supervision_population_by_po_by_day.md) <br/>
  - [supervision_population_due_for_release_by_po_by_day](../../views/vitals_report_views/supervision_population_due_for_release_by_po_by_day.md) <br/>
  - [timely_contact_by_po_by_day](../../views/vitals_report_views/timely_contact_by_po_by_day.md) <br/>

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
  - state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>

#### static_reference_tables
_Reference tables used by various views in BigQuery. May need to be updated manually for new states._
  - us_id_roster ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_id_roster)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_id_roster)) <br/>
  - us_me_cis_908_ccs_location ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_me_cis_908_ccs_location)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_me_cis_908_ccs_location)) <br/>

## METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

|                                        **Metric**                                        |      **US_ID**      |**US_ME**|      **US_MO**      |      **US_ND**      |      **US_PA**      |      **US_TN**      |
|------------------------------------------------------------------------------------------|---------------------|---------|---------------------|---------------------|---------------------|---------------------|
|[SUPERVISION_COMPLIANCE](../../metrics/supervision/supervision_case_compliance_metrics.md)|36 months            |         |                     |36 months            |36 months            |                     |
|[SUPERVISION_POPULATION](../../metrics/supervision/supervision_population_metrics.md)     |36 months 240 months |         |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
