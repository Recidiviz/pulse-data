# PUBLIC DASHBOARD
The Public Dashboard (Spotlight) is a public data portal designed to make criminal justice data more accessible to public stakeholders. It accomplishes this through a series of interactive data visualizations on key metrics (such as populations or admission reasons) and holistic, data-driven narratives on what's happening in the system as a whole.
## SHIPPED STATES
  - [Pennsylvania](../../states/pennsylvania.md)
  - [North Dakota](../../states/north_dakota.md)

## STATES IN DEVELOPMENT
  N/A

## VIEWS

#### public_dashboard_views
  - [active_program_participation_by_region](../../views/public_dashboard_views/active_program_participation_by_region.md) <br/>
  - [community_corrections_population_by_facility_by_demographics](../../views/public_dashboard_views/community_corrections_population_by_facility_by_demographics.md) <br/>
  - [incarceration_lengths_by_demographics](../../views/public_dashboard_views/incarceration_lengths_by_demographics.md) <br/>
  - [incarceration_population_by_admission_reason](../../views/public_dashboard_views/incarceration_population_by_admission_reason.md) <br/>
  - [incarceration_population_by_facility_by_demographics](../../views/public_dashboard_views/incarceration_population_by_facility_by_demographics.md) <br/>
  - [incarceration_population_by_month_by_demographics](../../views/public_dashboard_views/incarceration_population_by_month_by_demographics.md) <br/>
  - [incarceration_population_by_prioritized_race_and_ethnicity_by_period](../../views/public_dashboard_views/incarceration_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
  - [incarceration_releases_by_type_by_period](../../views/public_dashboard_views/incarceration_releases_by_type_by_period.md) <br/>
  - [racial_disparities](../../views/public_dashboard_views/racial_disparities.md) <br/>
  - [recidivism_rates_by_cohort_by_year](../../views/public_dashboard_views/recidivism_rates_by_cohort_by_year.md) <br/>
  - [sentence_type_by_district_by_demographics](../../views/public_dashboard_views/sentence_type_by_district_by_demographics.md) <br/>
  - [supervision_population_by_district_by_demographics](../../views/public_dashboard_views/supervision_population_by_district_by_demographics.md) <br/>
  - [supervision_population_by_month_by_demographics](../../views/public_dashboard_views/supervision_population_by_month_by_demographics.md) <br/>
  - [supervision_population_by_prioritized_race_and_ethnicity_by_period](../../views/public_dashboard_views/supervision_population_by_prioritized_race_and_ethnicity_by_period.md) <br/>
  - [supervision_revocations_by_period_by_type_by_demographics](../../views/public_dashboard_views/supervision_revocations_by_period_by_type_by_demographics.md) <br/>
  - [supervision_success_by_month](../../views/public_dashboard_views/supervision_success_by_month.md) <br/>
  - [supervision_success_by_period_by_demographics](../../views/public_dashboard_views/supervision_success_by_period_by_demographics.md) <br/>
  - [supervision_terminations_by_month](../../views/public_dashboard_views/supervision_terminations_by_month.md) <br/>
  - [supervision_terminations_by_period_by_demographics](../../views/public_dashboard_views/supervision_terminations_by_period_by_demographics.md) <br/>

#### shared_metric_views
  - [event_based_commitments_from_supervision](../../views/shared_metric_views/event_based_commitments_from_supervision.md) <br/>
  - [event_based_supervision_populations](../../views/shared_metric_views/event_based_supervision_populations.md) <br/>
  - [single_day_incarceration_population_for_spotlight](../../views/shared_metric_views/single_day_incarceration_population_for_spotlight.md) <br/>
  - [single_day_supervision_population_for_spotlight](../../views/shared_metric_views/single_day_supervision_population_for_spotlight.md) <br/>

#### us_nd_raw_data_up_to_date_views
  - docstars_REF_PROVIDER_LOCATION_latest ([Raw Data Doc](../../../ingest/us_nd/raw_data/docstars_REF_PROVIDER_LOCATION.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_nd_raw_data_up_to_date_views&t=docstars_REF_PROVIDER_LOCATION_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_nd_raw_data_up_to_date_views&t=docstars_REF_PROVIDER_LOCATION_latest)) <br/>

#### us_pa_supplemental
  - [recidivism](../../views/us_pa_supplemental/recidivism.md) <br/>

## SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

#### static_reference_tables
_Reference tables used by various views in BigQuery. May need to be updated manually for new states._
  - state_incarceration_facility_capacity ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_incarceration_facility_capacity)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_incarceration_facility_capacity)) <br/>
  - state_race_ethnicity_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_race_ethnicity_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_race_ethnicity_population_counts)) <br/>

## METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

|                                                         **Metric**                                                          |      **US_ID**      |      **US_ME**      |      **US_MO**      |      **US_ND**      |      **US_PA**      |      **US_TN**      |
|-----------------------------------------------------------------------------------------------------------------------------|---------------------|---------------------|---------------------|---------------------|---------------------|---------------------|
|[INCARCERATION_COMMITMENT_FROM_SUPERVISION](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md)|36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[INCARCERATION_POPULATION](../../metrics/incarceration/incarceration_population_metrics.md)                                  |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[INCARCERATION_RELEASE](../../metrics/incarceration/incarceration_release_metrics.md)                                        |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[PROGRAM_PARTICIPATION](../../metrics/program/program_participation_metrics.md)                                              |36 months 240 months |                     |                     |36 months 60 months  |                     |                     |
|[REINCARCERATION_RATE](../../metrics/recidivism/recidivism_rate_metrics.md)                                                  |                     |                     |                     |None months          |                     |                     |
|[SUPERVISION_POPULATION](../../metrics/supervision/supervision_population_metrics.md)                                        |36 months 240 months |                     |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
|[SUPERVISION_SUCCESS](../../metrics/supervision/supervision_success_metrics.md)                                              |                     |                     |                     |36 months 240 months |36 months            |                     |
|[SUPERVISION_TERMINATION](../../metrics/supervision/supervision_termination_metrics.md)                                      |36 months 240 months |                     |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
