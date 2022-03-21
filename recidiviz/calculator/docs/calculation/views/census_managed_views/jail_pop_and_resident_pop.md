## census_managed_views.jail_pop_and_resident_pop

Combines jail population and resident population counts into one table.
Joined based on fips-year-race-gender combinations.

All years <= 1999 are excluded.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=jail_pop_and_resident_pop)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=jail_pop_and_resident_pop)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.jail_pop_and_resident_pop](../census_managed_views/jail_pop_and_resident_pop.md) <br/>
|--[census_managed_views.resident_population_counts](../census_managed_views/resident_population_counts.md) <br/>
|----vera_data.iob_race_gender_pop ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=iob_race_gender_pop)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=iob_race_gender_pop)) <br/>
|--[census_managed_views.population_admissions_releases_race_gender_all](../census_managed_views/population_admissions_releases_race_gender_all.md) <br/>
|----[census_managed_views.population_admissions_releases_race_gender](../census_managed_views/population_admissions_releases_race_gender.md) <br/>
|------[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|--------vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|------census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>
|------census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>


##### Descendants
This view has no child dependencies.
