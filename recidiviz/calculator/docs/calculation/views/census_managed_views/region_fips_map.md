## census_managed_views.region_fips_map

A view mapping county fips codes to scraper region codes


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=region_fips_map)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=region_fips_map)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.region_fips_map](../census_managed_views/region_fips_map.md) <br/>
|--census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>


##### Descendants
[census_managed_views.region_fips_map](../census_managed_views/region_fips_map.md) <br/>
|--[census_managed_views.scraper_success_rate](../census_managed_views/scraper_success_rate.md) <br/>

