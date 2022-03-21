## census_managed_views.charges_and_severity

Assigns a numeric column "severity" to each charge.
Charge class severity is defined in `census_managed_views.charge_class_severity_ranks`.
The lower the number, the more severe the charge class (1 is most severe, 8 is least).


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=charges_and_severity)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=charges_and_severity)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.charges_and_severity](../census_managed_views/charges_and_severity.md) <br/>
|--[census_managed_views.charge_class_severity_ranks](../census_managed_views/charge_class_severity_ranks.md) <br/>
|--census.charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=charge)) <br/>


##### Descendants
[census_managed_views.charges_and_severity](../census_managed_views/charges_and_severity.md) <br/>
|--[census_managed_views.charge_severity_all_bookings](../census_managed_views/charge_severity_all_bookings.md) <br/>
|----[census_managed_views.charge_info_by_booking](../census_managed_views/charge_info_by_booking.md) <br/>
|------[census_managed_views.charge_info_all_bookings](../census_managed_views/charge_info_all_bookings.md) <br/>
|----[census_managed_views.charge_severity_counts_all_bookings](../census_managed_views/charge_severity_counts_all_bookings.md) <br/>

