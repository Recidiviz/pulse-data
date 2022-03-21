## dashboard_views.pathways_prison_dimension_combinations
Helper view providing all possible combinations of incarceration dimension values. Useful for building exhaustive views.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=pathways_prison_dimension_combinations)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=pathways_prison_dimension_combinations)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.pathways_prison_dimension_combinations](../dashboard_views/pathways_prison_dimension_combinations.md) <br/>
|--[dashboard_views.pathways_incarceration_location_name_map](../dashboard_views/pathways_incarceration_location_name_map.md) <br/>
|----[reference_views.incarceration_location_ids_to_names](../reference_views/incarceration_location_ids_to_names.md) <br/>
|------static_reference_tables.us_me_cis_908_ccs_location ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=us_me_cis_908_ccs_location)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=us_me_cis_908_ccs_location)) <br/>
|------external_reference.us_nd_incarceration_facility_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_nd_incarceration_facility_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_nd_incarceration_facility_names)) <br/>
|------external_reference.us_me_incarceration_facility_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_me_incarceration_facility_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_me_incarceration_facility_names)) <br/>
|------external_reference.us_id_incarceration_facility_names ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_incarceration_facility_names)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_incarceration_facility_names)) <br/>
|------external_reference.us_id_incarceration_facility_map ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=external_reference&t=us_id_incarceration_facility_map)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=external_reference&t=us_id_incarceration_facility_map)) <br/>


##### Descendants
[dashboard_views.pathways_prison_dimension_combinations](../dashboard_views/pathways_prison_dimension_combinations.md) <br/>
|--[dashboard_views.prison_population_time_series](../dashboard_views/prison_population_time_series.md) <br/>
|--[dashboard_views.prison_to_supervision_count_by_month](../dashboard_views/prison_to_supervision_count_by_month.md) <br/>

