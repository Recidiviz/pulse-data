## validation_views.multiple_supervision_info_for_commitment_admission
A view revealing when a commitment from supervision admission is associated with
more than one supervising_officer_external_id,
level_1_supervision_location_external_id,
or level_2_supervision_location_external_id.

A failure indicates a bug in the commitment from supervision identification 
calculation logic.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=multiple_supervision_info_for_commitment_admission)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=multiple_supervision_info_for_commitment_admission)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.multiple_supervision_info_for_commitment_admission](../validation_views/multiple_supervision_info_for_commitment_admission.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
