## analyst_data.offense_type_mapping
Reference view that creates a mapping from numerous offense_type categories to a smaller set of offense categories

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=analyst_data&t=offense_type_mapping)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=analyst_data&t=offense_type_mapping)
<br/>

#### Dependency Trees

##### Parentage
[analyst_data.offense_type_mapping](../analyst_data/offense_type_mapping.md) <br/>
|--us_tn_raw_data_up_to_date_views.OffenderStatute_latest ([Raw Data Doc](../../../ingest/us_tn/raw_data/OffenderStatute.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_tn_raw_data_up_to_date_views&t=OffenderStatute_latest)) <br/>
|--state.state_charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_charge)) <br/>


##### Descendants
[analyst_data.offense_type_mapping](../analyst_data/offense_type_mapping.md) <br/>
|--[sessions.compartment_sentences](../sessions/compartment_sentences.md) <br/>
|----[analyst_data.us_id_ppo_metrics_early_discharge_reduction](../analyst_data/us_id_ppo_metrics_early_discharge_reduction.md) <br/>
|----[externally_shared_views.csg_compartment_sentences](../externally_shared_views/csg_compartment_sentences.md) <br/>
|----[linestaff_data_validation.offense_types](../linestaff_data_validation/offense_types.md) <br/>
|------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|----[population_projection_data.incarceration_remaining_sentences](../population_projection_data/incarceration_remaining_sentences.md) <br/>
|------[population_projection_data.remaining_sentences](../population_projection_data/remaining_sentences.md) <br/>
|--[sessions.us_tn_compartment_sentences](../sessions/us_tn_compartment_sentences.md) <br/>
|----[sessions.compartment_sentences](../sessions/compartment_sentences.md) <br/>
|------[analyst_data.us_id_ppo_metrics_early_discharge_reduction](../analyst_data/us_id_ppo_metrics_early_discharge_reduction.md) <br/>
|------[externally_shared_views.csg_compartment_sentences](../externally_shared_views/csg_compartment_sentences.md) <br/>
|------[linestaff_data_validation.offense_types](../linestaff_data_validation/offense_types.md) <br/>
|--------[linestaff_data_validation.looker_dashboard](../linestaff_data_validation/looker_dashboard.md) <br/>
|------[population_projection_data.incarceration_remaining_sentences](../population_projection_data/incarceration_remaining_sentences.md) <br/>
|--------[population_projection_data.remaining_sentences](../population_projection_data/remaining_sentences.md) <br/>

