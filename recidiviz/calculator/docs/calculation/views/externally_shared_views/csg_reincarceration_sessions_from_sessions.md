## externally_shared_views.csg_reincarceration_sessions_from_sessions


## Overview

The table `reincarceration_sessions_from_sessions_materialized` joins to `compartment_sessions` and identifies incarceration releases and subsequent reincarcerations when / if they occur. The view has a record for every person and every incarceration release -  if there is a subsequent reincarceration the `session_id` and `reincarceration_date` associated with the reincarceration session will be populated.

This table can be used to calculate reincarceration rates based on the time between release and reincarceration as well as cumulative reincarceration curves by time since release. Additionally, since the release session id and reincarceration session id are both included in this view, this table can be joined back to sessions to determine characteristics associated with the release cohort (age, gender, etc) as well as the reincarceration (crime type, inflow from, etc) by joining to sessions. 

The logic is slightly more complex than just identifying incarceration sessions that follow an incarceration release, due to the way that states can have temporary parole board holds. The guidance that we have received is that these temporary holds should not count either as reincarcerations or as releases (in cases where the parole board does not revoke the person). The reincarceration should count when / if a person is revoked to an incarceration session following a parole board hold.  As an example, let’s say the person had the following set of sessions:

1. Incarceration - General Term
2. Parole
3. Incarceration - Parole Board Hold
4. Parole
5. Incarceration - Parole Board Hold
6. Incarceration - General Term

In the above example, session 1 would be the incarceration release session and session 6 would be the subsequent reincarceration session.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	state_code	|	State	|
|	release_session_id	|	Session ID for the incarceration session from which the person was released from. This includes any incarceration session (except for temporary parole board holds) that transition to release or supervision compartments	|
|	release_date	|	Date that the person was released (one day after the release session end date)	|
|	days_since_release	|	Days between the person's release date and the last day of data	|
|	months_since_release	|	Calendar count of full months between the last day of data and the release date. This field is generally used to identify the cohort to be used for a particular calculation (for example 12 month recidivism requires a cohort that has been released for at least 12 months). We can select that cohort using this field - months_since_release >=12 because a value of 12 is people that have been released a full 12 months.	|
|	years_since_release	|	Calendar count of full years between the last day of data and the release date.	|
|	reincarceration	|	Binary indicator for whether the person has been reincarcerated	|
|	reincarceration_session_id	|	Session ID of the reincarceration session	|
|	reincarceration_date	|	Date of the reincarceration	|
|	release_to_reincarceration_days	|	Days between release and reincarceration	|
|	release_to_reincarceration_months	|	Calendar months between release date and reincarceration date. These fields are generally used to determine whether the event occurred within that time period. As such, the value gets rounded up - a person that revoked 11 months and 5 days after their supervision start will get a value of 12 because they should count towards a 12 month revocation rate but not an 11 month revocation rate	|
|	release_to_reincarceration_years	|	Years between the release and reincarceration. This value is rounded up as months are, as described above	|

## Methodology

This view is constructed directly from sessions. The query does a self join where reincarceration sessions are left joined to incarceration sessions that end in release. The alias "release_session" refers to the session from which a person is released, and the alias "reincarceration_session" refers to the incarceration sessions that follow this release.

The table is deduped so that each person's release session is associated with their first reincarceration, if there are more than one. For example, if a person was incarcerated three distinct times, the third reincarceration gets associated with only the second release, not the first. 

Releases are identified as those incarceration sessions that meet the following criteria:

1. Outflow to `SUPERVISION` or `RELEASE`
2. Are not parole board hold sessions

Reincarcerations are then joined to these releases based on:

1. Being an incarceration session that is not a parole board hold
2. Starting after the end date of the last supervision
3. Not inflowing from another incarceration term, unless that incarceration term is a parole board hold


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=externally_shared_views&t=csg_reincarceration_sessions_from_sessions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=externally_shared_views&t=csg_reincarceration_sessions_from_sessions)
<br/>

#### Dependency Trees

##### Parentage
[externally_shared_views.csg_reincarceration_sessions_from_sessions](../externally_shared_views/csg_reincarceration_sessions_from_sessions.md) <br/>
|--[sessions.reincarceration_sessions_from_sessions](../sessions/reincarceration_sessions_from_sessions.md) <br/>
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
