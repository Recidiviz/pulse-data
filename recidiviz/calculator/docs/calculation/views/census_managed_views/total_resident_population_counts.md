## census_managed_views.total_resident_population_counts

A view that contains fips, year, total_resident_pop"
FROM bridged_race_population_estimates, renaming total_pop15to64 as 
total_resident_pop.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=total_resident_population_counts)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=total_resident_population_counts)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.total_resident_population_counts](../census_managed_views/total_resident_population_counts.md) <br/>
|--vera_data.bridged_race_population_estimates ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=bridged_race_population_estimates)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=bridged_race_population_estimates)) <br/>


##### Descendants
This view has no child dependencies.
