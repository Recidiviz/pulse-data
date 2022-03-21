## externally_shared_views.csg_supervision_super_sessions

## Overview

This view has a record for each person and each supervision super-session. A supervision super-session groups together compartment sessions that represent a continuous stay on supervision despite a person's legal status changing. The most frequent examples of this are (1) parole board holds where a person can either be revoked or returned to parole, and (2) bench warrants where a person often transitions from probation, to a bench warrant, and back to probation. 

The view has a couple of important uses:

1. Supervision LOS is probably more accurately calculated using this view because otherwise LOS will be artificially skewed down by transitions such as PAROLE --> PBH --> PAROLE or PROBATION --> BENCH_WARRANT --> PROBATION if we were to calculate LOS on those individual supervision sessions.
2. This view is used as the denominator for revocation rate calculations as well as the starting point to determine when a revocation occurs relative to the supervision start. The views `revocation_sessions` and `revocation_cohort_sessions` are based on a `supervision_super_session` as the unit of analysis from which a person can be revoked.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	supervision_super_session_id	|	Super session identifier	|
|	start_date	|	Super session start date	|
|	end_date	|	Super session end date	|
|	state_code	|	State	|
|	incarceration_days	|	Number of days of the super session that are spent incarcerated. Someone spends part of their supervision super-session incarcerated because of parole board holds and shock incarceration 	|
|	session_length_days	|	Difference between session start date and session end date. For active sessions the session start date is differenced from the last day of data	|
|	session_id_start	|	Compartment session id associated with the start of the super session. This field and the following field are used to join sessions and super-sessions	|
|	session_id_end	|	Compartment session id associated with the end of the super session. This field and the preceding field are used to join sessions and super-sessions	|
|	start_reason	|	Start reason associated with the start of a super session. This is pulled from the compartment session represented by `session_id_start`	|
|	start_sub_reason	|	Start sub reason associated with the start of a super session. This is pulled from the compartment session represented by `session_id_start`	|
|	end_reason	|	End associated with the start of a super session. This is pulled from the compartment session represented by `session_id_end`	|
|	inflow_from_level_1	|	Compartment level 1 value of the preceding compartment session	|
|	inflow_from_level_2	|	Compartment level 2 value of the preceding compartment session	|
|	outflow_to_level_1	|	Compartment level 1 value of the subsequent compartment session	|
|	outflow_to_level_2	|	Compartment level 2 value of the subsequent compartment session	|
|	last_day_of_data	|	The last day for which we have data, specific to a state. This is pulled from `compartment_sessions`	|

## Methodology

A supervision super session aggregates together sessions of the following types:

1. Any session with `compartment_level_1` in one of the following:

    1. `SUPERVISION` (this includes bench warrants and absconsions)
    2. `SUPERVISION_OUT_OF_STATE`
    
2. Any session with `compartment_level_2` in one of the following:

    1. `PAROLE_BOARD_HOLD`
    2. `PENDING_CUSTODY`
    3. `TEMPORARY_CUSTODY`
    4. `SUSPENSION`
    5. `SHOCK_INCARCERATION`


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=externally_shared_views&t=csg_supervision_super_sessions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=externally_shared_views&t=csg_supervision_super_sessions)
<br/>

#### Dependency Trees

##### Parentage
[externally_shared_views.csg_supervision_super_sessions](../externally_shared_views/csg_supervision_super_sessions.md) <br/>
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
This view has no child dependencies.
