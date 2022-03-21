## census_managed_views.population_admissions_releases_race_gender

For each day-fips combination,
compute the total population, admissions, and releases by race and gender.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=population_admissions_releases_race_gender)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=population_admissions_releases_race_gender)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.population_admissions_releases_race_gender](../census_managed_views/population_admissions_releases_race_gender.md) <br/>
|--[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|----vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>
|--census.person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=person)) <br/>
|--census.booking ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census&t=booking)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census&t=booking)) <br/>


##### Descendants
[census_managed_views.population_admissions_releases_race_gender](../census_managed_views/population_admissions_releases_race_gender.md) <br/>
|--[census_managed_views.population_admissions_releases](../census_managed_views/population_admissions_releases.md) <br/>
|--[census_managed_views.population_admissions_releases_race_gender_all](../census_managed_views/population_admissions_releases_race_gender_all.md) <br/>
|----[census_managed_views.jail_pop_and_resident_pop](../census_managed_views/jail_pop_and_resident_pop.md) <br/>

