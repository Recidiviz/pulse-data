## census_managed_views.bond_amounts_by_booking

Collapse all bonds for a booking into a Bond Amounts by Booking table.

If any of the booking's bonds are DENIED, consider the entire booking's bonds to be DENIED.
If any amount is present on the bonds of a non-DENIED booking, sum it as the total_bond_dollars.
If no amounts are present and the bond is not DENIED, consider the total booking bond amount UNKNOWN.

Constraints:
If a Bond has `total_bond_dollars`, it cannot be DENIED or UNKNOWN.
If a Bond's `total_bond_dollars` is NULL, it must be one of DENIED or UNKNOWN.
If a Bond is DENIED or UNKNOWN, its `total_bond_dollars` must be NULL.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=bond_amounts_by_booking)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=bond_amounts_by_booking)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.bond_amounts_by_booking](../census_managed_views/bond_amounts_by_booking.md) <br/>
|--[census_managed_views.bond_amounts_unknown_denied](../census_managed_views/bond_amounts_unknown_denied.md) <br/>
|----census.bond ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=bond)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=bond)) <br/>


##### Descendants
[census_managed_views.bond_amounts_by_booking](../census_managed_views/bond_amounts_by_booking.md) <br/>
|--[census_managed_views.bond_amounts_all_bookings](../census_managed_views/bond_amounts_all_bookings.md) <br/>
|----[census_managed_views.bond_amounts_all_bookings_bins](../census_managed_views/bond_amounts_all_bookings_bins.md) <br/>

