## covid_public_data.facility_metadata
Facility names, IDs, and additional metadata for all facilities for which we have case data.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data&t=facility_metadata)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data&t=facility_metadata)
<br/>

#### Dependency Trees

##### Parentage
[covid_public_data.facility_metadata](../covid_public_data/facility_metadata.md) <br/>
|--covid_public_data_reference_tables.facility_locations ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_locations)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_locations)) <br/>
|--covid_public_data_reference_tables.facility_attributes ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_attributes)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_attributes)) <br/>
|--covid_public_data_reference_tables.facility_alias ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_alias)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_alias)) <br/>
|--[covid_public_data.facility_case_data](../covid_public_data/facility_case_data.md) <br/>
|----covid_public_data_reference_tables.facility_alias ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=facility_alias)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=facility_alias)) <br/>
|----covid_public_data_reference_tables.covid_cases_by_facility ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=covid_public_data_reference_tables&t=covid_cases_by_facility)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=covid_public_data_reference_tables&t=covid_cases_by_facility)) <br/>


##### Descendants
This view has no child dependencies.
