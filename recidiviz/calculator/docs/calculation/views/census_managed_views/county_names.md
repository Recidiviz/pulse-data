## census_managed_views.county_names

A view that contains all unique combinations of
fips, state name, and county name from
Vera's Incarceration Trends dataset.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=census_managed_views&t=county_names)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=census_managed_views&t=county_names)
<br/>

#### Dependency Trees

##### Parentage
[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|--vera_data.incarceration_trends ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=vera_data&t=incarceration_trends)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=vera_data&t=incarceration_trends)) <br/>


##### Descendants
[census_managed_views.county_names](../census_managed_views/county_names.md) <br/>
|--[census_managed_views.bond_amounts_all_bookings](../census_managed_views/bond_amounts_all_bookings.md) <br/>
|----[census_managed_views.bond_amounts_all_bookings_bins](../census_managed_views/bond_amounts_all_bookings_bins.md) <br/>
|--[census_managed_views.bond_amounts_all_bookings_bins](../census_managed_views/bond_amounts_all_bookings_bins.md) <br/>
|--[census_managed_views.charge_info_all_bookings](../census_managed_views/charge_info_all_bookings.md) <br/>
|--[census_managed_views.charge_severity_counts_all_bookings](../census_managed_views/charge_severity_counts_all_bookings.md) <br/>
|--[census_managed_views.charge_text_counts](../census_managed_views/charge_text_counts.md) <br/>
|--[census_managed_views.population_admissions_releases_race_gender](../census_managed_views/population_admissions_releases_race_gender.md) <br/>
|----[census_managed_views.population_admissions_releases](../census_managed_views/population_admissions_releases.md) <br/>
|----[census_managed_views.population_admissions_releases_race_gender_all](../census_managed_views/population_admissions_releases_race_gender_all.md) <br/>
|------[census_managed_views.jail_pop_and_resident_pop](../census_managed_views/jail_pop_and_resident_pop.md) <br/>
|--[census_managed_views.urbanicity](../census_managed_views/urbanicity.md) <br/>

