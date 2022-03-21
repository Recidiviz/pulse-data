# CORE DASHBOARD
Core dashboard provides leadership with an understanding of community supervision and revocation trends.
## SHIPPED STATES
  - [North Dakota](../../states/north_dakota.md)

## STATES IN DEVELOPMENT
  N/A

## VIEWS

#### dashboard_views
  - [admissions_by_type_by_month](../../views/dashboard_views/admissions_by_type_by_month.md) <br/>
  - [admissions_by_type_by_period](../../views/dashboard_views/admissions_by_type_by_period.md) <br/>
  - [admissions_versus_releases_by_month](../../views/dashboard_views/admissions_versus_releases_by_month.md) <br/>
  - [admissions_versus_releases_by_period](../../views/dashboard_views/admissions_versus_releases_by_period.md) <br/>
  - [average_change_lsir_score_by_month](../../views/dashboard_views/average_change_lsir_score_by_month.md) <br/>
  - [average_change_lsir_score_by_period](../../views/dashboard_views/average_change_lsir_score_by_period.md) <br/>
  - [avg_days_at_liberty_by_month](../../views/dashboard_views/avg_days_at_liberty_by_month.md) <br/>
  - [case_terminations_by_type_by_month](../../views/dashboard_views/case_terminations_by_type_by_month.md) <br/>
  - [case_terminations_by_type_by_officer_by_period](../../views/dashboard_views/case_terminations_by_type_by_officer_by_period.md) <br/>
  - [ftr_referrals_by_age_by_period](../../views/dashboard_views/ftr_referrals_by_age_by_period.md) <br/>
  - [ftr_referrals_by_gender_by_period](../../views/dashboard_views/ftr_referrals_by_gender_by_period.md) <br/>
  - [ftr_referrals_by_lsir_by_period](../../views/dashboard_views/ftr_referrals_by_lsir_by_period.md) <br/>
  - [ftr_referrals_by_month](../../views/dashboard_views/ftr_referrals_by_month.md) <br/>
  - [ftr_referrals_by_participation_status](../../views/dashboard_views/ftr_referrals_by_participation_status.md) <br/>
  - [ftr_referrals_by_period](../../views/dashboard_views/ftr_referrals_by_period.md) <br/>
  - [ftr_referrals_by_race_and_ethnicity_by_period](../../views/dashboard_views/ftr_referrals_by_race_and_ethnicity_by_period.md) <br/>
  - [reincarceration_rate_by_stay_length](../../views/dashboard_views/reincarceration_rate_by_stay_length.md) <br/>
  - [reincarcerations_by_month](../../views/dashboard_views/reincarcerations_by_month.md) <br/>
  - [reincarcerations_by_period](../../views/dashboard_views/reincarcerations_by_period.md) <br/>
  - [revocations_by_month](../../views/dashboard_views/revocations_by_month.md) <br/>
  - [revocations_by_officer_by_period](../../views/dashboard_views/revocations_by_officer_by_period.md) <br/>
  - [revocations_by_period](../../views/dashboard_views/revocations_by_period.md) <br/>
  - [revocations_by_race_and_ethnicity_by_period](../../views/dashboard_views/revocations_by_race_and_ethnicity_by_period.md) <br/>
  - [revocations_by_site_id_by_period](../../views/dashboard_views/revocations_by_site_id_by_period.md) <br/>
  - [revocations_by_supervision_type_by_month](../../views/dashboard_views/revocations_by_supervision_type_by_month.md) <br/>
  - [revocations_by_violation_type_by_month](../../views/dashboard_views/revocations_by_violation_type_by_month.md) <br/>
  - [supervision_termination_by_type_by_month](../../views/dashboard_views/supervision_termination_by_type_by_month.md) <br/>
  - [supervision_termination_by_type_by_period](../../views/dashboard_views/supervision_termination_by_type_by_period.md) <br/>

#### reference_views
  - [augmented_agent_info](../../views/reference_views/augmented_agent_info.md) <br/>
  - [supervision_period_to_agent_association](../../views/reference_views/supervision_period_to_agent_association.md) <br/>

#### shared_metric_views
  - [event_based_admissions](../../views/shared_metric_views/event_based_admissions.md) <br/>
  - [event_based_commitments_from_supervision](../../views/shared_metric_views/event_based_commitments_from_supervision.md) <br/>
  - [event_based_program_referrals](../../views/shared_metric_views/event_based_program_referrals.md) <br/>
  - [event_based_supervision_populations](../../views/shared_metric_views/event_based_supervision_populations.md) <br/>
  - [event_based_supervision_populations_with_commitments_for_rate_denominators](../../views/shared_metric_views/event_based_supervision_populations_with_commitments_for_rate_denominators.md) <br/>

## SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

#### state
_Ingested state data. This dataset is a copy of the state postgres database._
  - state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
  - state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>

## METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

|                                                         **Metric**                                                          |      **US_ID**      |      **US_ME**      |      **US_MO**      |      **US_ND**      |      **US_PA**      |      **US_TN**      |
|-----------------------------------------------------------------------------------------------------------------------------|---------------------|---------------------|---------------------|---------------------|---------------------|---------------------|
|[INCARCERATION_ADMISSION](../../metrics/incarceration/incarceration_admission_metrics.md)                                    |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[INCARCERATION_COMMITMENT_FROM_SUPERVISION](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md)|36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[INCARCERATION_POPULATION](../../metrics/incarceration/incarceration_population_metrics.md)                                  |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[INCARCERATION_RELEASE](../../metrics/incarceration/incarceration_release_metrics.md)                                        |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |36 months 360 months |36 months 240 months |
|[PROGRAM_REFERRAL](../../metrics/program/program_referral_metrics.md)                                                        |36 months 240 months |                     |                     |36 months 60 months  |                     |                     |
|[REINCARCERATION_COUNT](../../metrics/recidivism/recidivism_count_metrics.md)                                                |                     |                     |                     |None months          |                     |                     |
|[REINCARCERATION_RATE](../../metrics/recidivism/recidivism_rate_metrics.md)                                                  |                     |                     |                     |None months          |                     |                     |
|[SUPERVISION_POPULATION](../../metrics/supervision/supervision_population_metrics.md)                                        |36 months 240 months |                     |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
|[SUPERVISION_SUCCESS](../../metrics/supervision/supervision_success_metrics.md)                                              |                     |                     |                     |36 months 240 months |36 months            |                     |
|[SUPERVISION_TERMINATION](../../metrics/supervision/supervision_termination_metrics.md)                                      |36 months 240 months |                     |36 months 240 months |36 months 240 months |36 months 240 months |36 months 240 months |
