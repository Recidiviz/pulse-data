## census_managed_views.charge_text_counts

Counts the number of bookings associated with each charge text
for every day-fips combination.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=charge_text_counts)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=charge_text_counts)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.charge_text_counts](../census_managed_views/charge_text_counts.md) <br/>
|--[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|----vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|--census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>
|--census.charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=charge)) <br/>
|--census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>


##### Descendants
This view has no child dependencies.
