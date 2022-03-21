## census_managed_views.bond_amounts_all_bookings

A complete table of total bond amounts,
and whether the bonds are UNKNOWN or DENIED, for every Booking.

Joins bond_amounts_by_booking with all of the Bookings,
and all of those bookings' People.

If no bonds are present for a Booking in bond_amounts_by_booking,
`total_bond_dollars` is NULL, `denied` is False, and `unknown` is True.

Constraints:
If a Booking has `total_bond_dollars`, it cannot be DENIED or UNKNOWN.
If a Booking's `total_bond_dollars` is NULL, it must be one of DENIED or UNKNOWN.
If a Booking is DENIED or UNKNOWN, its `total_bond_dollars` must be NULL.

Assertions:
The total number of distinct `booking_id`s in this table should be equal to
the total number of distinct  `booking_id`s in Booking.

The number of UNKNOWN Bookings must be equal to the sum of
UNKNOWN Bookings in `bond_amounts_by_booking`,
plus all the Bookings whose booking_id is not in Bonds.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=bond_amounts_all_bookings)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=bond_amounts_all_bookings)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.bond_amounts_all_bookings](../census_managed_views/bond_amounts_all_bookings.md) <br/>
|--[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|----vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|--[census_managed_views.bond_amounts_by_booking](../census_managed_views/bond_amounts_by_booking.md) <br/>
|----[census_managed_views.bond_amounts_unknown_denied](../census_managed_views/bond_amounts_unknown_denied.md) <br/>
|------census.bond ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=bond)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=bond)) <br/>
|--census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>
|--census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>


##### Descendants
[census_managed_views.bond_amounts_all_bookings](../census_managed_views/bond_amounts_all_bookings.md) <br/>
|--[census_managed_views.bond_amounts_all_bookings_bins](../census_managed_views/bond_amounts_all_bookings_bins.md) <br/>

