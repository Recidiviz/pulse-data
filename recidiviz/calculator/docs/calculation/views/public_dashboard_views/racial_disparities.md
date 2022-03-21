## public_dashboard_views.racial_disparities
Various metric counts broken down by race/ethnicity.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=racial_disparities)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=racial_disparities)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.racial_disparities](../public_dashboard_views/racial_disparities.md) <br/>
|--static_reference_tables.state_race_ethnicity_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_race_ethnicity_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_race_ethnicity_population_counts)) <br/>
|--[public_dashboard_views.supervision_revocations_by_period_by_type_by_demographics](../public_dashboard_views/supervision_revocations_by_period_by_type_by_demographics.md) <br/>
|----[shared_metric_views.event_based_commitments_from_supervision](../shared_metric_views/event_based_commitments_from_supervision.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>
|--[public_dashboard_views.supervision_population_by_prioritized_race_and_ethnicity_by_period](../public_dashboard_views/supervision_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
|----[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--[public_dashboard_views.supervision_population_by_district_by_demographics](../public_dashboard_views/supervision_population_by_district_by_demographics.md) <br/>
|----[shared_metric_views.single_day_supervision_population_for_spotlight](../shared_metric_views/single_day_supervision_population_for_spotlight.md) <br/>
|------[dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_single_day_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--[public_dashboard_views.sentence_type_by_district_by_demographics](../public_dashboard_views/sentence_type_by_district_by_demographics.md) <br/>
|----[shared_metric_views.single_day_supervision_population_for_spotlight](../shared_metric_views/single_day_supervision_population_for_spotlight.md) <br/>
|------[dataflow_metrics_materialized.most_recent_single_day_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_single_day_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|----[shared_metric_views.single_day_incarceration_population_for_spotlight](../shared_metric_views/single_day_incarceration_population_for_spotlight.md) <br/>
|------[dataflow_metrics_materialized.most_recent_single_day_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_single_day_incarceration_population_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--[public_dashboard_views.incarceration_releases_by_type_by_period](../public_dashboard_views/incarceration_releases_by_type_by_period.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|--[public_dashboard_views.incarceration_population_by_prioritized_race_and_ethnicity_by_period](../public_dashboard_views/incarceration_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
|----[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--[public_dashboard_views.active_program_participation_by_region](../public_dashboard_views/active_program_participation_by_region.md) <br/>
|----us_nd_raw_data_up_to_date_views.docstars_REF_PROVIDER_LOCATION_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/docstars_REF_PROVIDER_LOCATION.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=docstars_REF_PROVIDER_LOCATION_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=docstars_REF_PROVIDER_LOCATION_latest)) <br/>
|----[dataflow_metrics_materialized.most_recent_single_day_program_participation_metrics](../dataflow_metrics_materialized/most_recent_single_day_program_participation_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_program_participation_metrics](../dataflow_metrics_materialized/most_recent_program_participation_metrics.md) <br/>
|--------[dataflow_metrics.program_participation_metrics](../../metrics/program/program_participation_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
