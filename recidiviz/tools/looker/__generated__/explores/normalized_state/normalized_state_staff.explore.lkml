# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

explore: normalized_state_staff_template {
  extension: required
  extends: [
    normalized_state_staff_caseload_type_period,
    normalized_state_staff_external_id,
    normalized_state_staff_location_period,
    normalized_state_staff_role_period,
    normalized_state_staff_supervisor_period
  ]

  view_name: normalized_state_staff
  view_label: "normalized_state_staff"

  group_label: "State"
  join: normalized_state_staff_caseload_type_period {
    sql_on: ${normalized_state_staff.staff_id} = ${normalized_state_staff_caseload_type_period.staff_id};;
    relationship: one_to_many
  }

  join: normalized_state_staff_external_id {
    sql_on: ${normalized_state_staff.staff_id} = ${normalized_state_staff_external_id.staff_id};;
    relationship: one_to_many
  }

  join: normalized_state_staff_location_period {
    sql_on: ${normalized_state_staff.staff_id} = ${normalized_state_staff_location_period.staff_id};;
    relationship: one_to_many
  }

  join: normalized_state_staff_role_period {
    sql_on: ${normalized_state_staff.staff_id} = ${normalized_state_staff_role_period.staff_id};;
    relationship: one_to_many
  }

  join: normalized_state_staff_supervisor_period {
    sql_on: ${normalized_state_staff.staff_id} = ${normalized_state_staff_supervisor_period.staff_id};;
    relationship: one_to_many
  }

}
explore: normalized_state_staff_caseload_type_period {
  extension: required

}
explore: normalized_state_staff_external_id {
  extension: required

}
explore: normalized_state_staff_location_period {
  extension: required

}
explore: normalized_state_staff_role_period {
  extension: required

}
explore: normalized_state_staff_supervisor_period {
  extension: required

}
