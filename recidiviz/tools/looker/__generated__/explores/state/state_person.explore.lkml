# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

explore: state_person_template {
  extension: required
  extends: [
    state_assessment,
    state_drug_screen,
    state_employment_period,
    state_incarceration_incident,
    state_incarceration_period,
    state_incarceration_sentence,
    state_person_address_period,
    state_person_alias,
    state_person_ethnicity,
    state_person_external_id,
    state_person_housing_status_period,
    state_person_race,
    state_person_staff_relationship_period,
    state_program_assignment,
    state_sentence,
    state_sentence_group,
    state_supervision_contact,
    state_supervision_period,
    state_supervision_sentence,
    state_supervision_violation,
    state_task_deadline
  ]

  view_name: state_person
  view_label: "state_person"

  group_label: "State"
  join: state_person_periods {
    sql_on: ${state_person.person_id} = ${state_person_periods.person_id};;
    relationship: one_to_many
  }

  join: state_assessment {
    sql_on: ${state_person.person_id} = ${state_assessment.person_id};;
    relationship: one_to_many
  }

  join: state_drug_screen {
    sql_on: ${state_person.person_id} = ${state_drug_screen.person_id};;
    relationship: one_to_many
  }

  join: state_employment_period {
    sql_on: ${state_person.person_id} = ${state_employment_period.person_id};;
    relationship: one_to_many
  }

  join: state_incarceration_incident {
    sql_on: ${state_person.person_id} = ${state_incarceration_incident.person_id};;
    relationship: one_to_many
  }

  join: state_incarceration_period {
    sql_on: ${state_person.person_id} = ${state_incarceration_period.person_id};;
    relationship: one_to_many
  }

  join: state_incarceration_sentence {
    sql_on: ${state_person.person_id} = ${state_incarceration_sentence.person_id};;
    relationship: one_to_many
  }

  join: state_person_address_period {
    sql_on: ${state_person.person_id} = ${state_person_address_period.person_id};;
    relationship: one_to_many
  }

  join: state_person_alias {
    sql_on: ${state_person.person_id} = ${state_person_alias.person_id};;
    relationship: one_to_many
  }

  join: state_person_ethnicity {
    sql_on: ${state_person.person_id} = ${state_person_ethnicity.person_id};;
    relationship: one_to_many
  }

  join: state_person_external_id {
    sql_on: ${state_person.person_id} = ${state_person_external_id.person_id};;
    relationship: one_to_many
  }

  join: state_person_housing_status_period {
    sql_on: ${state_person.person_id} = ${state_person_housing_status_period.person_id};;
    relationship: one_to_many
  }

  join: state_person_race {
    sql_on: ${state_person.person_id} = ${state_person_race.person_id};;
    relationship: one_to_many
  }

  join: state_person_staff_relationship_period {
    sql_on: ${state_person.person_id} = ${state_person_staff_relationship_period.person_id};;
    relationship: one_to_many
  }

  join: state_program_assignment {
    sql_on: ${state_person.person_id} = ${state_program_assignment.person_id};;
    relationship: one_to_many
  }

  join: state_sentence {
    sql_on: ${state_person.person_id} = ${state_sentence.person_id};;
    relationship: one_to_many
  }

  join: state_sentence_group {
    sql_on: ${state_person.person_id} = ${state_sentence_group.person_id};;
    relationship: one_to_many
  }

  join: state_supervision_contact {
    sql_on: ${state_person.person_id} = ${state_supervision_contact.person_id};;
    relationship: one_to_many
  }

  join: state_supervision_period {
    sql_on: ${state_person.person_id} = ${state_supervision_period.person_id};;
    relationship: one_to_many
  }

  join: state_supervision_sentence {
    sql_on: ${state_person.person_id} = ${state_supervision_sentence.person_id};;
    relationship: one_to_many
  }

  join: state_supervision_violation {
    sql_on: ${state_person.person_id} = ${state_supervision_violation.person_id};;
    relationship: one_to_many
  }

  join: state_task_deadline {
    sql_on: ${state_person.person_id} = ${state_task_deadline.person_id};;
    relationship: one_to_many
  }

}
explore: state_assessment {
  extension: required

}
explore: state_drug_screen {
  extension: required

}
explore: state_employment_period {
  extension: required

}
explore: state_incarceration_incident {
  extension: required
  extends: [
    state_incarceration_incident_outcome
  ]

  join: state_incarceration_incident_outcome {
    sql_on: ${state_incarceration_incident.incarceration_incident_id} = ${state_incarceration_incident_outcome.incarceration_incident_id};;
    relationship: one_to_many
  }

}
explore: state_incarceration_incident_outcome {
  extension: required

}
explore: state_incarceration_period {
  extension: required

}
explore: state_incarceration_sentence {
  extension: required
  extends: [
    state_charge_incarceration_sentence_association,
    state_early_discharge
  ]

  join: state_charge_incarceration_sentence_association {
    sql_on: ${state_incarceration_sentence.incarceration_sentence_id} = ${state_charge_incarceration_sentence_association.incarceration_sentence_id};;
    relationship: one_to_many
  }

  join: state_early_discharge {
    sql_on: ${state_incarceration_sentence.incarceration_sentence_id} = ${state_early_discharge.incarceration_sentence_id};;
    relationship: one_to_many
  }

}
explore: state_charge_incarceration_sentence_association {
  extension: required

  join: state_charge {
    sql_on: ${state_charge_incarceration_sentence_association.charge_id} = ${state_charge.charge_id};;
    relationship: many_to_one
  }

}
explore: state_early_discharge {
  extension: required

}
explore: state_person_address_period {
  extension: required

}
explore: state_person_alias {
  extension: required

}
explore: state_person_ethnicity {
  extension: required

}
explore: state_person_external_id {
  extension: required

}
explore: state_person_housing_status_period {
  extension: required

}
explore: state_person_race {
  extension: required

}
explore: state_person_staff_relationship_period {
  extension: required

}
explore: state_program_assignment {
  extension: required

}
explore: state_sentence {
  extension: required
  extends: [
    state_charge_v2_state_sentence_association,
    state_sentence_length,
    state_sentence_status_snapshot
  ]

  join: state_charge_v2_state_sentence_association {
    sql_on: ${state_sentence.sentence_id} = ${state_charge_v2_state_sentence_association.sentence_id};;
    relationship: one_to_many
  }

  join: state_sentence_length {
    sql_on: ${state_sentence.sentence_id} = ${state_sentence_length.sentence_id};;
    relationship: one_to_many
  }

  join: state_sentence_status_snapshot {
    sql_on: ${state_sentence.sentence_id} = ${state_sentence_status_snapshot.sentence_id};;
    relationship: one_to_many
  }

}
explore: state_charge_v2_state_sentence_association {
  extension: required

  join: state_charge_v2 {
    sql_on: ${state_charge_v2_state_sentence_association.charge_v2_id} = ${state_charge_v2.charge_v2_id};;
    relationship: many_to_one
  }

}
explore: state_sentence_length {
  extension: required

}
explore: state_sentence_status_snapshot {
  extension: required

}
explore: state_sentence_group {
  extension: required
  extends: [
    state_sentence_group_length
  ]

  join: state_sentence_group_length {
    sql_on: ${state_sentence_group.sentence_group_id} = ${state_sentence_group_length.sentence_group_id};;
    relationship: one_to_many
  }

}
explore: state_sentence_group_length {
  extension: required

}
explore: state_supervision_contact {
  extension: required

}
explore: state_supervision_period {
  extension: required
  extends: [
    state_supervision_case_type_entry
  ]

  join: state_supervision_case_type_entry {
    sql_on: ${state_supervision_period.supervision_period_id} = ${state_supervision_case_type_entry.supervision_period_id};;
    relationship: one_to_many
  }

}
explore: state_supervision_case_type_entry {
  extension: required

}
explore: state_supervision_sentence {
  extension: required
  extends: [
    state_charge_supervision_sentence_association,
    state_early_discharge
  ]

  join: state_charge_supervision_sentence_association {
    sql_on: ${state_supervision_sentence.supervision_sentence_id} = ${state_charge_supervision_sentence_association.supervision_sentence_id};;
    relationship: one_to_many
  }

  join: state_early_discharge {
    sql_on: ${state_supervision_sentence.supervision_sentence_id} = ${state_early_discharge.supervision_sentence_id};;
    relationship: one_to_many
  }

}
explore: state_charge_supervision_sentence_association {
  extension: required

  join: state_charge {
    sql_on: ${state_charge_supervision_sentence_association.charge_id} = ${state_charge.charge_id};;
    relationship: many_to_one
  }

}
explore: state_supervision_violation {
  extension: required
  extends: [
    state_supervision_violated_condition_entry,
    state_supervision_violation_response,
    state_supervision_violation_type_entry
  ]

  join: state_supervision_violated_condition_entry {
    sql_on: ${state_supervision_violation.supervision_violation_id} = ${state_supervision_violated_condition_entry.supervision_violation_id};;
    relationship: one_to_many
  }

  join: state_supervision_violation_response {
    sql_on: ${state_supervision_violation.supervision_violation_id} = ${state_supervision_violation_response.supervision_violation_id};;
    relationship: one_to_many
  }

  join: state_supervision_violation_type_entry {
    sql_on: ${state_supervision_violation.supervision_violation_id} = ${state_supervision_violation_type_entry.supervision_violation_id};;
    relationship: one_to_many
  }

}
explore: state_supervision_violated_condition_entry {
  extension: required

}
explore: state_supervision_violation_response {
  extension: required
  extends: [
    state_supervision_violation_response_decision_entry
  ]

  join: state_supervision_violation_response_decision_entry {
    sql_on: ${state_supervision_violation_response.supervision_violation_response_id} = ${state_supervision_violation_response_decision_entry.supervision_violation_response_id};;
    relationship: one_to_many
  }

}
explore: state_supervision_violation_response_decision_entry {
  extension: required

}
explore: state_supervision_violation_type_entry {
  extension: required

}
explore: state_task_deadline {
  extension: required

}
