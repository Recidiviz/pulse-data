## census_managed_views.bond_amounts_all_bookings_bins

Adds a 'bond_amount_category' field to `bond_amounts_all_bookings`
with either 'DENIED', 'UNKNOWN', or bins from:
0-500
500-100
1000-10000
10000-25000
25000-100000
100000+

Also sums the counts of populations, admissions, and releases for each
day-fips-category grouping.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=bond_amounts_all_bookings_bins)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=bond_amounts_all_bookings_bins)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.bond_amounts_all_bookings_bins](../census_managed_views/bond_amounts_all_bookings_bins.md) <br/>
|--[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|----vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|--[census_managed_views.bond_amounts_all_bookings](../census_managed_views/bond_amounts_all_bookings.md) <br/>
|----[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|------vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|----[census_managed_views.bond_amounts_by_booking](../census_managed_views/bond_amounts_by_booking.md) <br/>
|------[census_managed_views.bond_amounts_unknown_denied](../census_managed_views/bond_amounts_unknown_denied.md) <br/>
|--------census.bond ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=bond)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=bond)) <br/>
|----census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>
|----census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>


##### Descendants
This view has no child dependencies.
