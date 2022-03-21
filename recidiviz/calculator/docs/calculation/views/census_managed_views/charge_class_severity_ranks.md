## census_managed_views.charge_class_severity_ranks

A View of all charge classes and their severity ranks.

Severity is ranked where 0 is most severe, and 7 is least severe.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=charge_class_severity_ranks)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=charge_class_severity_ranks)
<br/>

#### Dependency Trees

##### Parentage
This view has no parent dependencies.

##### Descendants
[census_managed_views.charge_class_severity_ranks](../census_managed_views/charge_class_severity_ranks.md) <br/>
|--[census_managed_views.charges_and_severity](../census_managed_views/charges_and_severity.md) <br/>
|----[census_managed_views.charge_severity_all_bookings](../census_managed_views/charge_severity_all_bookings.md) <br/>
|------[census_managed_views.charge_info_by_booking](../census_managed_views/charge_info_by_booking.md) <br/>
|--------[census_managed_views.charge_info_all_bookings](../census_managed_views/charge_info_all_bookings.md) <br/>
|------[census_managed_views.charge_severity_counts_all_bookings](../census_managed_views/charge_severity_counts_all_bookings.md) <br/>

