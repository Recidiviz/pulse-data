## census_managed_views.charge_info_all_bookings

A complete table of total charge class and levels, for every Booking.

Joins charge_info_by_booking with all of the Bookings,
and all of those bookings' People.

Assertions:
The total number of distinct `booking_id`s in this table should be equal to
the total number of distinct  `booking_id`s in Booking.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=charge_info_all_bookings)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=charge_info_all_bookings)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.charge_info_all_bookings](../census_managed_views/charge_info_all_bookings.md) <br/>
|--[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|----vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|--[census_managed_views.charge_info_by_booking](../census_managed_views/charge_info_by_booking.md) <br/>
|----[census_managed_views.charge_severity_all_bookings](../census_managed_views/charge_severity_all_bookings.md) <br/>
|------[census_managed_views.charges_and_severity](../census_managed_views/charges_and_severity.md) <br/>
|--------[census_managed_views.charge_class_severity_ranks](../census_managed_views/charge_class_severity_ranks.md) <br/>
|--------census.charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=charge)) <br/>
|------census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>
|----census.charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=charge)) <br/>
|--census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>
|--census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>


##### Descendants
This view has no child dependencies.
