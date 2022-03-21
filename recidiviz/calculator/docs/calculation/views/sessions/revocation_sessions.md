## sessions.revocation_sessions

## Overview

The view `revocation_sessions_materialized` has a record for each person and each supervision super session. It joins back to `compartment_sessions` and identifies supervision starts and subsequent revocations when / if they occur. The table has a record for every supervision start and if there is a subsequent revocation the session_id and revocation date associated with the revocation session will be populated. 

This table can be used to calculate revocation rates based on the time between a supervision start and revocation. Additionally, since the supervision session id and revocation session id are both included in this view, this table can be joined back to `compartment_sessions` to pull characteristics associated with the supervision cohort (age, gender, etc) as well as the revocation (violation type). 

The logic is slightly more complex than just looking at supervision sessions that transition to an incarceration session because of the fact that states can have temporary parole board holds which don’t necessarily lead to a revocation. 

As an example, let’s say a person had the following set of sessions:

1. Incarceration - General Term
2. Parole
3. Incarceration - Parole Board Hold
4. Parole
5. Incarceration - Parole Board Hold
6. Incarceration - General Term

In this example, the supervision session ID would be session 2 and the revocation session ID would be 6. This person spent time incarcerated while on two parole board holds, one of which led to a revocation. In this example we would compare the start date of session 6 to the start date of session 2 if looking to understand the timing of when the revocation occurred relative to their release from prison.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	state_code	|	State	|
|	supervision_super_session_id	|	An ID that groups together continuous stays on supervision, in cases where an individual may go on PAROLE_BOARD_HOLD and back to Parole several times before being revoked	|
|	supervision_session_id	|	Session ID associated with the supervision session that a person was either released or sentenced to	|
|	supervision_start_date	|	Start date of the supervision session. This is the date to which revocation dates are compared to in determining the amount of time spent on supervision prior to a revocation	|
|	days_since_start	|	Days between the person's supervision start date and the last day of data	|
|	months_since_start	|	Calendar count of full months between a person's supervision start date and the last day of data. This field is generally used to identify the cohort to be used for a particular calculation (for example 12 month revocation rate requires a cohort that has started supervision at least 12 months prior). We can select that cohort using this field - months_since_start >=12 because a value of 12 is people that started their supervision period more than 12 months ago	|
|	years_since_start	|	Calendar count of full years between the last day of data and the supervision start date.	|
|	revocation_date	|	If the supervision super session outflows to incarceration, this is the date that their incarceration session started	|
|	revocation	|	Binary revocation indicator to count revocations (1 if the supervision super session ended in a revocation, otherwise 0)	|
|	revocation_session_id	|	The session id associated with the incarceration session that starts because of a revocation	|
|	supervision_start_to_revocation_days	|	Days between the supervision session start and the start of incarceration session that starts because of a revocation	|
|	supervision_start_to_revocation_months	|	Months between the supervision session start and the start of the incarceration session that starts because of a revocation. These fields are generally used to determine whether the event occurred within that time period. As such, the value gets rounded up - a person that revoked 11 months and 5 days after their supervision start will get a value of 12 because they should count towards a 12 month revocation rate but not an 11 month revocation rate	|
|	supervision_start_to_revocation_years		Years between the supervision session start and the start of incarceration session that starts because of a revocation. This value is rounded up as months are, as described above	|


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sessions&t=revocation_sessions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sessions&t=revocation_sessions)
<br/>

#### Dependency Trees

##### Parentage
[sessions.revocation_sessions](../sessions/revocation_sessions.md) <br/>
|--[sessions.supervision_super_sessions](../sessions/supervision_super_sessions.md) <br/>
|----[sessions.compartment_sessions](../sessions/compartment_sessions.md) <br/>
|------static_reference_tables.session_inferred_start_reasons_materialized ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=session_inferred_start_reasons_materialized)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=session_inferred_start_reasons_materialized)) <br/>
|------static_reference_tables.session_inferred_end_reasons_materialized ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=session_inferred_end_reasons_materialized)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=session_inferred_end_reasons_materialized)) <br/>
|------[sessions.supervision_level_dedup_priority](../sessions/supervision_level_dedup_priority.md) <br/>
|------[sessions.person_demographics](../sessions/person_demographics.md) <br/>
|--------static_reference_tables.state_race_ethnicity_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_race_ethnicity_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_race_ethnicity_population_counts)) <br/>
|--------state.state_person_race ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_race)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_race)) <br/>
|--------state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|--------state.state_person_ethnicity ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_ethnicity)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_ethnicity)) <br/>
|--------state.state_person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person)) <br/>
|------[sessions.dataflow_sessions](../sessions/dataflow_sessions.md) <br/>
|--------[sessions.us_tn_judicial_district_sessions](../sessions/us_tn_judicial_district_sessions.md) <br/>
|----------[sessions.us_tn_sentences_preprocessed](../sessions/us_tn_sentences_preprocessed.md) <br/>
|------------us_tn_raw_data_up_to_date_views.Sentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/Sentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) <br/>
|------------us_tn_raw_data_up_to_date_views.SentenceMiscellaneous_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/SentenceMiscellaneous.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) <br/>
|------------us_tn_raw_data_up_to_date_views.OffenderStatute_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/OffenderStatute.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) <br/>
|------------us_tn_raw_data_up_to_date_views.JOSentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOSentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) <br/>
|------------us_tn_raw_data_up_to_date_views.JOIdentification_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOIdentification.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) <br/>
|------------us_tn_raw_data_up_to_date_views.JOCharge_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOCharge.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) <br/>
|------------state.state_supervision_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_sentence)) <br/>
|------------state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|------------state.state_incarceration_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_sentence)) <br/>
|--------[sessions.us_tn_incarceration_population_metrics_preprocessed](../sessions/us_tn_incarceration_population_metrics_preprocessed.md) <br/>
|----------state.state_incarceration_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_period)) <br/>
|----------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|------------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--------[sessions.us_mo_supervision_population_metrics_preprocessed](../sessions/us_mo_supervision_population_metrics_preprocessed.md) <br/>
|----------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--------[sessions.us_id_supervision_population_metrics_preprocessed](../sessions/us_id_supervision_population_metrics_preprocessed.md) <br/>
|----------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--------[sessions.us_id_supervision_out_of_state_population_metrics_preprocessed](../sessions/us_id_supervision_out_of_state_population_metrics_preprocessed.md) <br/>
|----------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|------------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|--------[sessions.us_id_incarceration_population_metrics_preprocessed](../sessions/us_id_incarceration_population_metrics_preprocessed.md) <br/>
|----------static_reference_tables.state_incarceration_facilities ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_incarceration_facilities)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_incarceration_facilities)) <br/>
|----------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|------------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_not_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|------[sessions.compartment_session_start_reasons](../sessions/compartment_session_start_reasons.md) <br/>
|--------[sessions.admission_start_reason_dedup_priority](../sessions/admission_start_reason_dedup_priority.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|----------[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_start_metrics](../dataflow_metrics_materialized/most_recent_supervision_start_metrics.md) <br/>
|----------[dataflow_metrics.supervision_start_metrics](../../metrics/supervision/supervision_start_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>
|------[sessions.compartment_session_end_reasons](../sessions/compartment_session_end_reasons.md) <br/>
|--------[sessions.release_termination_reason_dedup_priority](../sessions/release_termination_reason_dedup_priority.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|----------[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_start_metrics](../dataflow_metrics_materialized/most_recent_supervision_start_metrics.md) <br/>
|----------[dataflow_metrics.supervision_start_metrics](../../metrics/supervision/supervision_start_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>
|------[sessions.compartment_level_2_dedup_priority](../sessions/compartment_level_2_dedup_priority.md) <br/>
|------[sessions.compartment_level_1_dedup_priority](../sessions/compartment_level_1_dedup_priority.md) <br/>
|------[sessions.assessment_score_sessions](../sessions/assessment_score_sessions.md) <br/>
|--------state.state_assessment ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_assessment)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_assessment)) <br/>


##### Descendants
[sessions.revocation_sessions](../sessions/revocation_sessions.md) <br/>
|--[analyst_data.event_based_metrics_by_supervision_officer](../analyst_data/event_based_metrics_by_supervision_officer.md) <br/>
|----[analyst_data.event_based_metrics_by_district](../analyst_data/event_based_metrics_by_district.md) <br/>
|--[analyst_data.supervision_officer_caseload_health_metrics](../analyst_data/supervision_officer_caseload_health_metrics.md) <br/>
|--[externally_shared_views.csg_revocation_sessions](../externally_shared_views/csg_revocation_sessions.md) <br/>
|--[sessions.revocation_cohort_sessions](../sessions/revocation_cohort_sessions.md) <br/>
|--[validation_views.revocation_sessions_to_dataflow_disaggregated](../validation_views/revocation_sessions_to_dataflow_disaggregated.md) <br/>

