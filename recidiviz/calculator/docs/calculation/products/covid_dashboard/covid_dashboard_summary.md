# COVID DASHBOARD
COVID dashboard aids corrections leadership in their COVID response. The web app models the spread of COVID through jails and prisons and the projected impact on COVID cases and community hospital resources. Scenario modeling enables users to understand the impact of their interventions on the projected spread.
## SHIPPED STATES
  N/A

## STATES IN DEVELOPMENT
  N/A

## VIEWS

#### covid_public_data
  - [facility_case_data](../../views/covid_public_data/facility_case_data.md) <br/>
  - [facility_metadata](../../views/covid_public_data/facility_metadata.md) <br/>

## SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

#### covid_public_data_reference_tables
_Reference tables used by the COVID dashboard. Updated manually._
  - covid_cases_by_facility ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=covid_cases_by_facility)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=covid_cases_by_facility)) <br/>
  - facility_alias ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_alias)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_alias)) <br/>
  - facility_attributes ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_attributes)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_attributes)) <br/>
  - facility_locations ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_locations)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_locations)) <br/>

## METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

*This product does not rely on Dataflow metrics.*
