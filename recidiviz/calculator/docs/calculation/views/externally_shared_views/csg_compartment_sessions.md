## externally_shared_views.csg_compartment_sessions

## Overview

Compartment sessions is our most frequently used sessionized view. This table is unique on `person_id` and `session_id`. In this view a new session is triggered every time a `compartment_level_1` or `compartment_level_2` value changes. A "compartment" can generally be defined as someone's legal status within incarceration or supervision.

In the data we’ve constructed we have a hierarchy of compartment level 1 and compartment level 2. The main compartment level 1 values are used to represent incarceration and supervision. Level 2 values further categorize the legal status. For example, the supervision level 1 sessions most frequently have level 2 values of probation or parole. This view also has columns for the preceding and ensuing sessions, referred to as "inflow" and "outflow" compartments.

It is important to note that a compartment does not always equal legal status. There have been a number of cases where for analysis or reporting purposes it has been useful for us to define these slightly differently. For example, excluded facilities in ID trigger their own new compartment (`INCARCERATION_OUT_OF_STATE`) even if a person is of the same legal status because we want to be able to view those events separately.

Compartment sessions differs from other sessionized views in that the edges should correspond with dataflow start and end events incarceration admission metrics, incarceration releases, supervision starts, and supervision terminations). As such, we leverage these start/end reason values to make inference in cases where we have missing data (a person shows up in neither incarceration or supervision population metrics). The following are the `compartment_level_1` values that are generated from population metrics:

1. `INCARCERATION`
2. `SUPERVISION`
3. `SUPERVISION_OUT_OF_STATE`

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	session_id	|	Ordered session number per person	|
|	dataflow_session_id_start	|	Session ID from `dataflow_sessions` at start of compartment session. Dataflow sessions is a more finely disaggregated view of sessions where a new session is triggered by a change in any attribute (compartment, location, supervision officer, case type, etc)	|
|	dataflow_session_id_end	|	Session ID from `dataflow_sessions` at end of compartment session	|
|	start_date	|	Day that a person's session starts. This will correspond with the incarceration admission or supervision start date	|
|	end_date	|	Last full-day of a person's session. The release or termination date will be the `end_date` + 1 day	|
|	state_code	|	State	|
|	compartment_level_1	|	Level 1 Compartment. Possible values are: <br>-`INCARCERATION`<br>-`INCARCERATION_OUT_OF_STATE`<br>-`SUPERVISION`<br>-`SUPERVISION_OUT_OF_STATE`<br>-`RELEASE`<br>-`INTERNAL_UNKNOWN`, <br>-`PENDING_CUSTODY`<br>-`PENDING_SUPERVISION`<br>-`SUSPENSION`<br>ERRONEOUS_RELEASE	|
|	compartment_level_2	|	Level 2 Compartment. Possible values for the incarceration compartments are: <br>-`GENERAL`<br>-`PAROLE_BOARD_HOLD`<br>-`TREATMENT_IN_PRISON` <br>-`SHOCK_INCARCERATION`<br>-`ABSCONSION`<br>-`INTERNAL_UNKNOWN`<br>-`COMMUNITY_CONFINEMENT`<br>-`TEMPORARY_CUSTODY`<br><br>Possible values for the supervision compartments are: <br>-`PROBATION`<br>-`PAROLE`<br>-`ABSCONSION`<br>-`DUAL`<br>-`BENCH_WARRANT`<br>-`INFORMAL_PROBATION`<br>-`INTERNAL_UNKNOWN`<br><br>All other `compartment_level_1` values have the same value for `compartment_level_1` and `comparmtent_level_2`	|
|	session_length_days	|	Length of session in days. For active sessions this is the number of days between session start and the most recent day of data. The minimum value of this field is `1` in cases where the person has the same `start_date` and `end_date` (they spent one full day in the compartment)	|
|	session_days_inferred	|	The number of days (out of the total `session_length_days`) that are inferred. This type onference happens when we have a gap between the same compartment with no dataflow start/end events indicating a compartment transition. As an example let's say someone was in GENERAL for 30 days, then we had a gap in population data of 5 days, and then they were in GENERAL again for 30 days. If there are no dataflow events at the transition edges, we infer that that person was in GENERAL for that entire time. They would have a session length of 65 days with a value of `5` for `session_days_inferred`	|
|	start_reason	|	Reason for the session start. This is pulled from `compartment_session_start_reasons` which is generated from the union of the incarceration admission and supervision start dataflow metrics. Start events are deduplicated to unique person/days within supervision and within incarceration and then joined to sessions generated from the population metric. This field is not fully hydrated.	|
|	start_sub_reason	|	This field represents the most severe violation associated with an admission and is only populated for incarceration commitments from supervision, which includes both revocation admissions and sanction admissions.	|
|	end_reason	|	Reason for the session end. This is pulled from `compartment_session_end_reasons` which is generated from the union of the incarceration release and supervision termination dataflow metrics. End events are deduplicated to unique person/days within supervision and within incarceration and then joined to sessions generated from the population metric. If a session is currently active this field will be NULL. This field is not fully hydrated.	|
|	is_inferred_start_reason	|	Indicator for whether the start reason is inferred based on the transition inflow	|
|	is_inferred_end_reason	|	Indicator for whether the end reason is inferred based on the transition outflow	|
|	start_reason_original	|	Original start reason (will bet the same as the start reason unless the original is overwritten because of inference)	|
|	end_reason_original	|	Original end reason (will bet the same as the end reason unless the original is overwritten because of inference)	|
|	earliest_start_date	|	The first date, across all sessions, that a person appears in our population data	|
|	last_day_of_data	|	The last day for which we have data, specific to a state. The is is calculated as the state min of the max day of which we have population data across supervision and population metrics within a state. For example, if in ID the max incarceration population date value is 2021-09-01 and the max supervision population date value is 2021-09-02, we would say that the last day of data for ID is 2021-09-01.	|
|	inflow_from_level_1	|	Compartment level 1 value of the preceding session	|
|	inflow_from_level_2	|	Compartment level 2 value of the preceding session	|
|	outflow_to_level_1	|	Compartment level 1 value of the subsequent session	|
|	outflow_to_level_2	|	Compartment level 2 value of the subsequent session	|
|	compartment_location_start	|	Facility or supervision district at the start of the session	|
|	compartment_location_end	|	Facility or supervision district at the end of the session	|
|	correctional_level_start	|	A person's custody level (for incarceration sessions) or supervision level (for supervision sessions) as of the start of the session	|
|	correctional_level_end	|	A person's custody level (for incarceration sessions) or supervision level (for supervision sessions) as of the end of the session	|
|	age_start	|	Age at start of session	|
|	age_end	|	Age at end of session	|
|	gender	|	Gender	|
|	prioritized_race_or_ethnicity	|	Person's race or ethnicity. In cases where multiple race / ethnicities are listed, the least represented one for that state is chosen	|
|	assessment_score_start	|	Assessment score at start of session	|
|	assessment_score_end	|	Assessment score at end of session	|
|	supervising_officer_external_id_start	|	Supervision officer at start of session (only populated for supervision sessions)	|
|	supervising_officer_external_id_end	|	Supervision officer at end of session (only populated for supervision sessions)	|
|	age_bucket_start	|	Age bucket at start of session	|
|	age_bucket_end	|	Age bucket at end of session	|
|	assessment_score_bucket_start	|	Assessment score bucket at start of session	|
|	assessment_score_bucket_end	|	Asessment score bucket at end of session	|

## Methodology

At a high-level, the following steps are taken to generate `compartment_sessions`

1. Aggregate dataflow sessions to compartment sessions
    
    1. Uses the `compartment_level_1` and `compartment_level_2` fields to identify when these values change and creates a new `session_id` every time they do within a `person_id`

2. Join to dataflow start/end reasons
    
    1. Session start dates are joined to `compartment_session_start_reasons_materialized` and session end dates are joined to `compartment_session_end_reasons_materialized`. These views already handle deduplication in cases where there is more than one event on a given person/day. 

3. Use start/end reasons to infer compartment values when there are gaps in population data
    
    1. There are two categories of sessions compartment inference worth distinguishing: 
        
        1. **Cases that lead to further aggregation**. This is occurs when we have a gap in data with identical compartment values for the periods surrounding that gap. If this occurs _and_ there are no matching dataflow events that indicate a transition, we assume that this is missing data and that this gap should take on the compartment values of its neighboring sessions. When this occurs, those adjacent sessions will then be re-aggregated into one larger compartment session. The `session_days_inferred` can be used to identify compartment sessions that have some portion of its time inferred by this methodology 
        2. **Cases that take on an inferred compartment value**. This refers to cases where the session gap gets a new compartment value (not equal to one of our population metric-derived values) based on the dataflow start/end reasons. The most common and straightforward example of this is the `RELEASE` compartment. This occurs when we have a gap in the data where the preceding session end reason is one that would indicate the person leaving the system (`SENTENCE_SERVED`, `COMMUTED`, `DISCHARGE`, `EXPIRATION`, `PARDONED`). The full set of inferred compartment values is listed below along with the criteria used to determine compartment values.
            1. `RELEASE` - preceding end reason indicates transition to liberty
            2. `PENDING_CUSTODY` - preceding end reason of is a supervision session ending in revocation or the subsequent start reason indicates a revocation or sanction admission
            3. `PENDING_SUPERVISION` - previous end reason is a incarceration session ending in conditional release
            4. `SUSPENSION` - previous end reason is suspension
            5. `ERRONEOUS_RELEASE` - previous end reason or subsequent start reason indicate erroneous release
            6. `INCARCERATION_OUT_OF_STATE` - previous end reason indicates an incarceration transfer out of state
            7. `INTERNAL_UNKNOWN` - the value given to any gap between sessions that does not meet one of the above criteria
            
4. Use transitions and corresponding look-up table to infer start/end reasons in cases where there is no dataflow event that joins to the transition
    1. Transitions that eligible for inferred start/end reasons are maintained in `static_reference_tables.session_inferred_start_reasons_materialized` and `static_reference_tables.session_inferred_end_reasons_materialized`. The non-materialized view version of these tables maintain a live connection with the Google Sheet "Compartment Session Inferred Transition Reasons" which is located in the DADS shared folder. Materialized versions are then created by running the SQL script `update_static_reference_inferred_start_end_reasons.sql`, which is located with the rest of the sessions views.
    2. The inferred transition look-up tables specify compartment level 1 and level 2 values and corresponding transition compartments (inflows for start reasons and outflows for end reasons). Cases where this transition is observed without the valid specified start/end reason will take on the value specified in this table. The field `original_start_reason` indicates in what cases the original value gets overwritten. This can be specified as "ALL" (any start reason / end reason gets overwritten); a specific start / end reason (only transitions with that value will be overwritten); or left blank (only cases where the start/end reason is missing will it be inferred).

4. Join back to dataflow sessions and other demographic tables to get session characteristics
    
    1. Lastly, the re-aggregated compartment sessions are joined back to other views to add additional session characteristics 


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=externally_shared_views&t=csg_compartment_sessions)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=externally_shared_views&t=csg_compartment_sessions)
<br/>

#### Dependency Trees

##### Parentage
[externally_shared_views.csg_compartment_sessions](../externally_shared_views/csg_compartment_sessions.md) <br/>
|--[sessions.compartment_sessions](../sessions/compartment_sessions.md) <br/>
|----static_reference_tables.session_inferred_start_reasons_materialized ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=session_inferred_start_reasons_materialized)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=session_inferred_start_reasons_materialized)) <br/>
|----static_reference_tables.session_inferred_end_reasons_materialized ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=session_inferred_end_reasons_materialized)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=session_inferred_end_reasons_materialized)) <br/>
|----[sessions.supervision_level_dedup_priority](../sessions/supervision_level_dedup_priority.md) <br/>
|----[sessions.person_demographics](../sessions/person_demographics.md) <br/>
|------static_reference_tables.state_race_ethnicity_population_counts ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_race_ethnicity_population_counts)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_race_ethnicity_population_counts)) <br/>
|------state.state_person_race ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_race)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_race)) <br/>
|------state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|------state.state_person_ethnicity ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_ethnicity)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_ethnicity)) <br/>
|------state.state_person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person)) <br/>
|----[sessions.dataflow_sessions](../sessions/dataflow_sessions.md) <br/>
|------[sessions.us_tn_judicial_district_sessions](../sessions/us_tn_judicial_district_sessions.md) <br/>
|--------[sessions.us_tn_sentences_preprocessed](../sessions/us_tn_sentences_preprocessed.md) <br/>
|----------us_tn_raw_data_up_to_date_views.Sentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/Sentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) <br/>
|----------us_tn_raw_data_up_to_date_views.SentenceMiscellaneous_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/SentenceMiscellaneous.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) <br/>
|----------us_tn_raw_data_up_to_date_views.OffenderStatute_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/OffenderStatute.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) <br/>
|----------us_tn_raw_data_up_to_date_views.JOSentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOSentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) <br/>
|----------us_tn_raw_data_up_to_date_views.JOIdentification_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOIdentification.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) <br/>
|----------us_tn_raw_data_up_to_date_views.JOCharge_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOCharge.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) <br/>
|----------state.state_supervision_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_sentence)) <br/>
|----------state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|----------state.state_incarceration_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_sentence)) <br/>
|------[sessions.us_tn_incarceration_population_metrics_preprocessed](../sessions/us_tn_incarceration_population_metrics_preprocessed.md) <br/>
|--------state.state_incarceration_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_period)) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|------[sessions.us_mo_supervision_population_metrics_preprocessed](../sessions/us_mo_supervision_population_metrics_preprocessed.md) <br/>
|--------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|------[sessions.us_id_supervision_population_metrics_preprocessed](../sessions/us_id_supervision_population_metrics_preprocessed.md) <br/>
|--------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|------[sessions.us_id_supervision_out_of_state_population_metrics_preprocessed](../sessions/us_id_supervision_out_of_state_population_metrics_preprocessed.md) <br/>
|--------state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|--------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|----------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|------[sessions.us_id_incarceration_population_metrics_preprocessed](../sessions/us_id_incarceration_population_metrics_preprocessed.md) <br/>
|--------static_reference_tables.state_incarceration_facilities ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_incarceration_facilities)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_incarceration_facilities)) <br/>
|--------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|----------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_not_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_not_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|----[sessions.compartment_session_start_reasons](../sessions/compartment_session_start_reasons.md) <br/>
|------[sessions.admission_start_reason_dedup_priority](../sessions/admission_start_reason_dedup_priority.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|--------[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_start_metrics](../dataflow_metrics_materialized/most_recent_supervision_start_metrics.md) <br/>
|--------[dataflow_metrics.supervision_start_metrics](../../metrics/supervision/supervision_start_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>
|----[sessions.compartment_session_end_reasons](../sessions/compartment_session_end_reasons.md) <br/>
|------[sessions.release_termination_reason_dedup_priority](../sessions/release_termination_reason_dedup_priority.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_termination_metrics](../dataflow_metrics_materialized/most_recent_supervision_termination_metrics.md) <br/>
|--------[dataflow_metrics.supervision_termination_metrics](../../metrics/supervision/supervision_termination_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_start_metrics](../dataflow_metrics_materialized/most_recent_supervision_start_metrics.md) <br/>
|--------[dataflow_metrics.supervision_start_metrics](../../metrics/supervision/supervision_start_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_supervision_out_of_state_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_out_of_state_population_metrics.md) <br/>
|--------[dataflow_metrics.supervision_out_of_state_population_metrics](../../metrics/supervision/supervision_out_of_state_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_population_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_population_metrics](../../metrics/incarceration/incarceration_population_metrics.md) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_admission_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_admission_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_admission_metrics](../../metrics/incarceration/incarceration_admission_metrics.md) <br/>
|----[sessions.compartment_level_2_dedup_priority](../sessions/compartment_level_2_dedup_priority.md) <br/>
|----[sessions.compartment_level_1_dedup_priority](../sessions/compartment_level_1_dedup_priority.md) <br/>
|----[sessions.assessment_score_sessions](../sessions/assessment_score_sessions.md) <br/>
|------state.state_assessment ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_assessment)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_assessment)) <br/>


##### Descendants
This view has no child dependencies.
