## census_managed_views.bond_amounts_unknown_denied

Create a Bond Amount table where:
If Bond.amount_dollars is present, keep it.
If Bond.bond_type = 'NOT_REQUIRED' OR Bond.status = 'POSTED',
then set Bond.amount_dollars to 0.

If after that, Bond.amount_dollars is NULL, we must set one of
the `unknown` or `denied` columns to True,
according to the following rules:

`unknown` = True in the following cases:
1) Bond.amount_dollars IS NULL AND Bond.bond_type = 'SECURED'
2) Bond.amount_dollars IS NULL
     AND Bond.bond_type IS NULL
       AND Bond.status = 'PRESENT_WITHOUT_INFO'

`denied` = True in the following cases:
1) Bond.amount_dollars IS NULL AND Bond.bond_type = 'DENIED'
2) Bond.amount_dollars IS NULL
     AND Bond.bond_type IS NULL
       AND Bond.status = 'REVOKED'

Constraints:
Only one of `unknown` and `denied` can be True.
If `amount_dollars` is not NULL,  `unknown` and `denied` must both be False.

NOTE: This may be easier or more readable if unknown/denied are written
as an enum unknown_or_denied column, then broken out into BOOL columns.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=bond_amounts_unknown_denied)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=bond_amounts_unknown_denied)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.bond_amounts_unknown_denied](../census_managed_views/bond_amounts_unknown_denied.md) <br/>
|--census.bond ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=bond)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=bond)) <br/>


##### Descendants
[census_managed_views.bond_amounts_unknown_denied](../census_managed_views/bond_amounts_unknown_denied.md) <br/>
|--[census_managed_views.bond_amounts_by_booking](../census_managed_views/bond_amounts_by_booking.md) <br/>
|----[census_managed_views.bond_amounts_all_bookings](../census_managed_views/bond_amounts_all_bookings.md) <br/>
|------[census_managed_views.bond_amounts_all_bookings_bins](../census_managed_views/bond_amounts_all_bookings_bins.md) <br/>

