# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

explore: normalized_state_person_template {
  extension: required
  extends: [
    normalized_state_assessment,
    normalized_state_drug_screen,
    normalized_state_employment_period,
    normalized_state_incarceration_incident,
    normalized_state_incarceration_period,
    normalized_state_incarceration_sentence,
    normalized_state_person_address_period,
    normalized_state_person_alias,
    normalized_state_person_ethnicity,
    normalized_state_person_external_id,
    normalized_state_person_housing_status_period,
    normalized_state_person_race,
    normalized_state_person_staff_relationship_period,
    normalized_state_program_assignment,
    normalized_state_sentence,
    normalized_state_sentence_group,
    normalized_state_sentence_imposed_group,
    normalized_state_sentence_inferred_group,
    normalized_state_supervision_contact,
    normalized_state_supervision_period,
    normalized_state_supervision_sentence,
    normalized_state_supervision_violation,
    normalized_state_task_deadline
  ]

  view_name: normalized_state_person
  view_label: "normalized_state_person"

  group_label: "State"
  join: normalized_state_person_periods {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_periods.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_assessment {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_assessment.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_drug_screen {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_drug_screen.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_employment_period {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_employment_period.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_incarceration_incident {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_incarceration_incident.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_incarceration_period {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_incarceration_period.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_incarceration_sentence {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_incarceration_sentence.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_address_period {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_address_period.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_alias {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_alias.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_ethnicity {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_ethnicity.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_external_id {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_external_id.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_housing_status_period {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_housing_status_period.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_race {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_race.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_person_staff_relationship_period {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_person_staff_relationship_period.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_program_assignment {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_program_assignment.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_sentence {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_sentence.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_sentence_group {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_sentence_group.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_sentence_imposed_group {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_sentence_imposed_group.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_sentence_inferred_group {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_sentence_inferred_group.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_supervision_contact {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_supervision_contact.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_supervision_period {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_supervision_period.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_supervision_sentence {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_supervision_sentence.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_supervision_violation {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_supervision_violation.person_id};;
    relationship: one_to_many
  }

  join: normalized_state_task_deadline {
    sql_on: ${normalized_state_person.person_id} = ${normalized_state_task_deadline.person_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_assessment {
  extension: required

}
explore: normalized_state_drug_screen {
  extension: required

}
explore: normalized_state_employment_period {
  extension: required

}
explore: normalized_state_incarceration_incident {
  extension: required
  extends: [
    normalized_state_incarceration_incident_outcome
  ]

  join: normalized_state_incarceration_incident_outcome {
    sql_on: ${normalized_state_incarceration_incident.incarceration_incident_id} = ${normalized_state_incarceration_incident_outcome.incarceration_incident_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_incarceration_incident_outcome {
  extension: required

}
explore: normalized_state_incarceration_period {
  extension: required

}
explore: normalized_state_incarceration_sentence {
  extension: required
  extends: [
    normalized_state_charge_incarceration_sentence_association,
    normalized_state_early_discharge
  ]

  join: normalized_state_charge_incarceration_sentence_association {
    sql_on: ${normalized_state_incarceration_sentence.incarceration_sentence_id} = ${normalized_state_charge_incarceration_sentence_association.incarceration_sentence_id};;
    relationship: one_to_many
  }

  join: normalized_state_early_discharge {
    sql_on: ${normalized_state_incarceration_sentence.incarceration_sentence_id} = ${normalized_state_early_discharge.incarceration_sentence_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_charge_incarceration_sentence_association {
  extension: required

  join: normalized_state_charge {
    sql_on: ${normalized_state_charge_incarceration_sentence_association.charge_id} = ${normalized_state_charge.charge_id};;
    relationship: many_to_one
  }

}
explore: normalized_state_early_discharge {
  extension: required

}
explore: normalized_state_person_address_period {
  extension: required

}
explore: normalized_state_person_alias {
  extension: required

}
explore: normalized_state_person_ethnicity {
  extension: required

}
explore: normalized_state_person_external_id {
  extension: required

}
explore: normalized_state_person_housing_status_period {
  extension: required

}
explore: normalized_state_person_race {
  extension: required

}
explore: normalized_state_person_staff_relationship_period {
  extension: required

}
explore: normalized_state_program_assignment {
  extension: required

}
explore: normalized_state_sentence {
  extension: required
  extends: [
    normalized_state_charge_v2_state_sentence_association,
    normalized_state_sentence_length,
    normalized_state_sentence_status_snapshot
  ]

  join: normalized_state_charge_v2_state_sentence_association {
    sql_on: ${normalized_state_sentence.sentence_id} = ${normalized_state_charge_v2_state_sentence_association.sentence_id};;
    relationship: one_to_many
  }

  join: normalized_state_sentence_length {
    sql_on: ${normalized_state_sentence.sentence_id} = ${normalized_state_sentence_length.sentence_id};;
    relationship: one_to_many
  }

  join: normalized_state_sentence_status_snapshot {
    sql_on: ${normalized_state_sentence.sentence_id} = ${normalized_state_sentence_status_snapshot.sentence_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_charge_v2_state_sentence_association {
  extension: required

  join: normalized_state_charge_v2 {
    sql_on: ${normalized_state_charge_v2_state_sentence_association.charge_v2_id} = ${normalized_state_charge_v2.charge_v2_id};;
    relationship: many_to_one
  }

}
explore: normalized_state_sentence_length {
  extension: required

}
explore: normalized_state_sentence_status_snapshot {
  extension: required

}
explore: normalized_state_sentence_group {
  extension: required
  extends: [
    normalized_state_sentence_group_length
  ]

  join: normalized_state_sentence_group_length {
    sql_on: ${normalized_state_sentence_group.sentence_group_id} = ${normalized_state_sentence_group_length.sentence_group_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_sentence_group_length {
  extension: required

}
explore: normalized_state_sentence_imposed_group {
  extension: required

}
explore: normalized_state_sentence_inferred_group {
  extension: required

}
explore: normalized_state_supervision_contact {
  extension: required

}
explore: normalized_state_supervision_period {
  extension: required
  extends: [
    normalized_state_supervision_case_type_entry
  ]

  join: normalized_state_supervision_case_type_entry {
    sql_on: ${normalized_state_supervision_period.supervision_period_id} = ${normalized_state_supervision_case_type_entry.supervision_period_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_supervision_case_type_entry {
  extension: required

}
explore: normalized_state_supervision_sentence {
  extension: required
  extends: [
    normalized_state_charge_supervision_sentence_association,
    normalized_state_early_discharge
  ]

  join: normalized_state_charge_supervision_sentence_association {
    sql_on: ${normalized_state_supervision_sentence.supervision_sentence_id} = ${normalized_state_charge_supervision_sentence_association.supervision_sentence_id};;
    relationship: one_to_many
  }

  join: normalized_state_early_discharge {
    sql_on: ${normalized_state_supervision_sentence.supervision_sentence_id} = ${normalized_state_early_discharge.supervision_sentence_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_charge_supervision_sentence_association {
  extension: required

  join: normalized_state_charge {
    sql_on: ${normalized_state_charge_supervision_sentence_association.charge_id} = ${normalized_state_charge.charge_id};;
    relationship: many_to_one
  }

}
explore: normalized_state_supervision_violation {
  extension: required
  extends: [
    normalized_state_supervision_violated_condition_entry,
    normalized_state_supervision_violation_response,
    normalized_state_supervision_violation_type_entry
  ]

  join: normalized_state_supervision_violated_condition_entry {
    sql_on: ${normalized_state_supervision_violation.supervision_violation_id} = ${normalized_state_supervision_violated_condition_entry.supervision_violation_id};;
    relationship: one_to_many
  }

  join: normalized_state_supervision_violation_response {
    sql_on: ${normalized_state_supervision_violation.supervision_violation_id} = ${normalized_state_supervision_violation_response.supervision_violation_id};;
    relationship: one_to_many
  }

  join: normalized_state_supervision_violation_type_entry {
    sql_on: ${normalized_state_supervision_violation.supervision_violation_id} = ${normalized_state_supervision_violation_type_entry.supervision_violation_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_supervision_violated_condition_entry {
  extension: required

}
explore: normalized_state_supervision_violation_response {
  extension: required
  extends: [
    normalized_state_supervision_violation_response_decision_entry
  ]

  join: normalized_state_supervision_violation_response_decision_entry {
    sql_on: ${normalized_state_supervision_violation_response.supervision_violation_response_id} = ${normalized_state_supervision_violation_response_decision_entry.supervision_violation_response_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_supervision_violation_response_decision_entry {
  extension: required

}
explore: normalized_state_supervision_violation_type_entry {
  extension: required

}
explore: normalized_state_task_deadline {
  extension: required

}
