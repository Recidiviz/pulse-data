## census_managed_views.bond_is_unique

A convenience view that lists each bond_id.
is_unique is true if the bond is only attached to one charge and false otherwise,
i.e. if it is shared between multiple charges.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=bond_is_unique)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=bond_is_unique)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.bond_is_unique](../census_managed_views/bond_is_unique.md) <br/>
|--census.charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=charge)) <br/>
|--census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>
|--census.bond ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=bond)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=bond)) <br/>


##### Descendants
This view has no child dependencies.
