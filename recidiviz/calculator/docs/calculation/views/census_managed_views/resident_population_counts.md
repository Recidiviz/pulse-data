## census_managed_views.resident_population_counts

Creates a resident population count table from
Vera's Incarceration Trends dataset. For every
year-fips-race-gender combination, there will be a `resident_pop` column.
Additionally, a `total_resident_pop` column is created that sums the entire
resident population for each year-fips combination.

Combines 'OTHER' and 'EXTERNAL_UNKNOWN' Race into 'OTHER/UNKNOWN'.

Create fake 'ALL' enums for Race and Gender fields.

Gender: 'ALL' sums Male, Female, and Unknown for each race.

Similarly, Race: 'ALL' sums every race for each gender.

DO NOT sum along race or gender, or you will double-count by including 'ALL'.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=resident_population_counts)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=resident_population_counts)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.resident_population_counts](../census_managed_views/resident_population_counts.md) <br/>
|--vera_data.iob_race_gender_pop ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=iob_race_gender_pop)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=iob_race_gender_pop)) <br/>


##### Descendants
[census_managed_views.resident_population_counts](../census_managed_views/resident_population_counts.md) <br/>
|--[census_managed_views.jail_pop_and_resident_pop](../census_managed_views/jail_pop_and_resident_pop.md) <br/>

