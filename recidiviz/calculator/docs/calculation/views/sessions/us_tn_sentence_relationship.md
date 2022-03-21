## sessions.us_tn_sentence_relationship
View that summarizes the relationship of TN sentences and defines sentence groups

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sessions&t=us_tn_sentence_relationship)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sessions&t=us_tn_sentence_relationship)
<br/>

#### Dependency Trees

##### Parentage
[sessions.us_tn_sentence_relationship](../sessions/us_tn_sentence_relationship.md) <br/>
|--[sessions.us_tn_sentences_preprocessed](../sessions/us_tn_sentences_preprocessed.md) <br/>
|----us_tn_raw_data_up_to_date_views.Sentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/Sentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=Sentence_latest)) <br/>
|----us_tn_raw_data_up_to_date_views.SentenceMiscellaneous_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/SentenceMiscellaneous.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=SentenceMiscellaneous_latest)) <br/>
|----us_tn_raw_data_up_to_date_views.OffenderStatute_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/OffenderStatute.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) <br/>
|----us_tn_raw_data_up_to_date_views.JOSentence_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOSentence.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOSentence_latest)) <br/>
|----us_tn_raw_data_up_to_date_views.JOIdentification_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOIdentification.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOIdentification_latest)) <br/>
|----us_tn_raw_data_up_to_date_views.JOCharge_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/JOCharge.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=JOCharge_latest)) <br/>
|----state.state_supervision_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_sentence)) <br/>
|----state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|----state.state_incarceration_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_sentence)) <br/>


##### Descendants
[sessions.us_tn_sentence_relationship](../sessions/us_tn_sentence_relationship.md) <br/>
|--[sessions.us_tn_compartment_sentences](../sessions/us_tn_compartment_sentences.md) <br/>
|----[sessions.compartment_sentences](../sessions/compartment_sentences.md) <br/>
|------[analyst_data.us_id_ppo_metrics_early_discharge_reduction](../analyst_data/us_id_ppo_metrics_early_discharge_reduction.md) <br/>
|------[externally_shared_views.csg_compartment_sentences](../externally_shared_views/csg_compartment_sentences.md) <br/>
|------[linestaff_data_validation.offense_types](../linestaff_data_validation/offense_types.md) <br/>
|--------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|------[population_projection_data.incarceration_remaining_sentences](../population_projection_data/incarceration_remaining_sentences.md) <br/>
|--------[population_projection_data.remaining_sentences](../population_projection_data/remaining_sentences.md) <br/>
|--[sessions.us_tn_sentence_summary](../sessions/us_tn_sentence_summary.md) <br/>
|----[sessions.us_tn_compartment_sentences](../sessions/us_tn_compartment_sentences.md) <br/>
|------[sessions.compartment_sentences](../sessions/compartment_sentences.md) <br/>
|--------[analyst_data.us_id_ppo_metrics_early_discharge_reduction](../analyst_data/us_id_ppo_metrics_early_discharge_reduction.md) <br/>
|--------[externally_shared_views.csg_compartment_sentences](../externally_shared_views/csg_compartment_sentences.md) <br/>
|--------[linestaff_data_validation.offense_types](../linestaff_data_validation/offense_types.md) <br/>
|----------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|--------[population_projection_data.incarceration_remaining_sentences](../population_projection_data/incarceration_remaining_sentences.md) <br/>
|----------[population_projection_data.remaining_sentences](../population_projection_data/remaining_sentences.md) <br/>

