## validation_views.normalized_entities_unique_ids
Reveals when normalization pipelines
are creating entities with duplicate ID values.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=normalized_entities_unique_ids)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=normalized_entities_unique_ids)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.normalized_entities_unique_ids](../validation_views/normalized_entities_unique_ids.md) <br/>
|--[us_tn_normalized_state.state_supervision_violation_type_entry](../us_tn_normalized_state/state_supervision_violation_type_entry.md) <br/>
|--[us_tn_normalized_state.state_supervision_violation_response_decision_entry](../us_tn_normalized_state/state_supervision_violation_response_decision_entry.md) <br/>
|--[us_tn_normalized_state.state_supervision_violation_response](../us_tn_normalized_state/state_supervision_violation_response.md) <br/>
|--[us_tn_normalized_state.state_supervision_violation](../us_tn_normalized_state/state_supervision_violation.md) <br/>
|--[us_tn_normalized_state.state_supervision_violated_condition_entry](../us_tn_normalized_state/state_supervision_violated_condition_entry.md) <br/>
|--[us_tn_normalized_state.state_supervision_period](../us_tn_normalized_state/state_supervision_period.md) <br/>
|--[us_tn_normalized_state.state_supervision_case_type_entry](../us_tn_normalized_state/state_supervision_case_type_entry.md) <br/>
|--[us_tn_normalized_state.state_program_assignment](../us_tn_normalized_state/state_program_assignment.md) <br/>
|--[us_tn_normalized_state.state_incarceration_period](../us_tn_normalized_state/state_incarceration_period.md) <br/>
|--[us_pa_normalized_state.state_supervision_violation_type_entry](../us_pa_normalized_state/state_supervision_violation_type_entry.md) <br/>
|--[us_pa_normalized_state.state_supervision_violation_response_decision_entry](../us_pa_normalized_state/state_supervision_violation_response_decision_entry.md) <br/>
|--[us_pa_normalized_state.state_supervision_violation_response](../us_pa_normalized_state/state_supervision_violation_response.md) <br/>
|--[us_pa_normalized_state.state_supervision_violation](../us_pa_normalized_state/state_supervision_violation.md) <br/>
|--[us_pa_normalized_state.state_supervision_violated_condition_entry](../us_pa_normalized_state/state_supervision_violated_condition_entry.md) <br/>
|--[us_pa_normalized_state.state_supervision_period](../us_pa_normalized_state/state_supervision_period.md) <br/>
|--[us_pa_normalized_state.state_supervision_case_type_entry](../us_pa_normalized_state/state_supervision_case_type_entry.md) <br/>
|--[us_pa_normalized_state.state_program_assignment](../us_pa_normalized_state/state_program_assignment.md) <br/>
|--[us_pa_normalized_state.state_incarceration_period](../us_pa_normalized_state/state_incarceration_period.md) <br/>
|--[us_nd_normalized_state.state_supervision_violation_type_entry](../us_nd_normalized_state/state_supervision_violation_type_entry.md) <br/>
|--[us_nd_normalized_state.state_supervision_violation_response_decision_entry](../us_nd_normalized_state/state_supervision_violation_response_decision_entry.md) <br/>
|--[us_nd_normalized_state.state_supervision_violation_response](../us_nd_normalized_state/state_supervision_violation_response.md) <br/>
|--[us_nd_normalized_state.state_supervision_violation](../us_nd_normalized_state/state_supervision_violation.md) <br/>
|--[us_nd_normalized_state.state_supervision_violated_condition_entry](../us_nd_normalized_state/state_supervision_violated_condition_entry.md) <br/>
|--[us_nd_normalized_state.state_supervision_period](../us_nd_normalized_state/state_supervision_period.md) <br/>
|--[us_nd_normalized_state.state_supervision_case_type_entry](../us_nd_normalized_state/state_supervision_case_type_entry.md) <br/>
|--[us_nd_normalized_state.state_program_assignment](../us_nd_normalized_state/state_program_assignment.md) <br/>
|--[us_nd_normalized_state.state_incarceration_period](../us_nd_normalized_state/state_incarceration_period.md) <br/>
|--[us_mo_normalized_state.state_supervision_violation_type_entry](../us_mo_normalized_state/state_supervision_violation_type_entry.md) <br/>
|--[us_mo_normalized_state.state_supervision_violation_response_decision_entry](../us_mo_normalized_state/state_supervision_violation_response_decision_entry.md) <br/>
|--[us_mo_normalized_state.state_supervision_violation_response](../us_mo_normalized_state/state_supervision_violation_response.md) <br/>
|--[us_mo_normalized_state.state_supervision_violation](../us_mo_normalized_state/state_supervision_violation.md) <br/>
|--[us_mo_normalized_state.state_supervision_violated_condition_entry](../us_mo_normalized_state/state_supervision_violated_condition_entry.md) <br/>
|--[us_mo_normalized_state.state_supervision_period](../us_mo_normalized_state/state_supervision_period.md) <br/>
|--[us_mo_normalized_state.state_supervision_case_type_entry](../us_mo_normalized_state/state_supervision_case_type_entry.md) <br/>
|--[us_mo_normalized_state.state_program_assignment](../us_mo_normalized_state/state_program_assignment.md) <br/>
|--[us_mo_normalized_state.state_incarceration_period](../us_mo_normalized_state/state_incarceration_period.md) <br/>
|--[us_me_normalized_state.state_supervision_violation_type_entry](../us_me_normalized_state/state_supervision_violation_type_entry.md) <br/>
|--[us_me_normalized_state.state_supervision_violation_response_decision_entry](../us_me_normalized_state/state_supervision_violation_response_decision_entry.md) <br/>
|--[us_me_normalized_state.state_supervision_violation_response](../us_me_normalized_state/state_supervision_violation_response.md) <br/>
|--[us_me_normalized_state.state_supervision_violation](../us_me_normalized_state/state_supervision_violation.md) <br/>
|--[us_me_normalized_state.state_supervision_violated_condition_entry](../us_me_normalized_state/state_supervision_violated_condition_entry.md) <br/>
|--[us_me_normalized_state.state_supervision_period](../us_me_normalized_state/state_supervision_period.md) <br/>
|--[us_me_normalized_state.state_supervision_case_type_entry](../us_me_normalized_state/state_supervision_case_type_entry.md) <br/>
|--[us_me_normalized_state.state_program_assignment](../us_me_normalized_state/state_program_assignment.md) <br/>
|--[us_me_normalized_state.state_incarceration_period](../us_me_normalized_state/state_incarceration_period.md) <br/>
|--[us_id_normalized_state.state_supervision_violation_type_entry](../us_id_normalized_state/state_supervision_violation_type_entry.md) <br/>
|--[us_id_normalized_state.state_supervision_violation_response_decision_entry](../us_id_normalized_state/state_supervision_violation_response_decision_entry.md) <br/>
|--[us_id_normalized_state.state_supervision_violation_response](../us_id_normalized_state/state_supervision_violation_response.md) <br/>
|--[us_id_normalized_state.state_supervision_violation](../us_id_normalized_state/state_supervision_violation.md) <br/>
|--[us_id_normalized_state.state_supervision_violated_condition_entry](../us_id_normalized_state/state_supervision_violated_condition_entry.md) <br/>
|--[us_id_normalized_state.state_supervision_period](../us_id_normalized_state/state_supervision_period.md) <br/>
|--[us_id_normalized_state.state_supervision_case_type_entry](../us_id_normalized_state/state_supervision_case_type_entry.md) <br/>
|--[us_id_normalized_state.state_program_assignment](../us_id_normalized_state/state_program_assignment.md) <br/>
|--[us_id_normalized_state.state_incarceration_period](../us_id_normalized_state/state_incarceration_period.md) <br/>


##### Descendants
This view has no child dependencies.
