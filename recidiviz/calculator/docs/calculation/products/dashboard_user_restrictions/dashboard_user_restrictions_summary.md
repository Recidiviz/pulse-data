# DASHBOARD USER RESTRICTIONS
User restrictions for the UP dashboard, including Lantern and Case Triage.
## SHIPPED STATES
  - [Missouri](../../states/missouri.md)
  - [Idaho](../../states/idaho.md)
  - [North Dakota](../../states/north_dakota.md)
  - [Maine](../../states/maine.md)
  - [Tennessee](../../states/tennessee.md)

## STATES IN DEVELOPMENT
  N/A

## VIEWS

#### reference_views
  - [dashboard_user_restrictions](../../views/reference_views/dashboard_user_restrictions.md) <br/>

#### us_mo_raw_data_up_to_date_views
  - LANTERN_DA_RA_LIST_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LANTERN_DA_RA_LIST.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LANTERN_DA_RA_LIST_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LANTERN_DA_RA_LIST_latest)) <br/>

## SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

#### static_reference_tables
_Reference tables used by various views in BigQuery. May need to be updated manually for new states._
  - recidiviz_unified_product_test_users ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=recidiviz_unified_product_test_users)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=recidiviz_unified_product_test_users)) <br/>
  - us_id_leadership_users ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_id_leadership_users)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_id_leadership_users)) <br/>
  - us_id_roster ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_id_roster)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_id_roster)) <br/>
  - us_me_leadership_users ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_me_leadership_users)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_me_leadership_users)) <br/>
  - us_nd_leadership_users ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_nd_leadership_users)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_nd_leadership_users)) <br/>
  - us_tn_leadership_users ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_tn_leadership_users)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_tn_leadership_users)) <br/>

## METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

*This product does not rely on Dataflow metrics.*
