# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

explore: state_staff_template {
  extension: required
  extends: [
    state_staff_caseload_type_period,
    state_staff_external_id,
    state_staff_location_period,
    state_staff_role_period,
    state_staff_supervisor_period
  ]

  view_name: state_staff
  view_label: "state_staff"

  group_label: "State"
  join: state_staff_caseload_type_period {
    sql_on: ${state_staff.staff_id} = ${state_staff_caseload_type_period.staff_id};;
    relationship: one_to_many
  }

  join: state_staff_external_id {
    sql_on: ${state_staff.staff_id} = ${state_staff_external_id.staff_id};;
    relationship: one_to_many
  }

  join: state_staff_location_period {
    sql_on: ${state_staff.staff_id} = ${state_staff_location_period.staff_id};;
    relationship: one_to_many
  }

  join: state_staff_role_period {
    sql_on: ${state_staff.staff_id} = ${state_staff_role_period.staff_id};;
    relationship: one_to_many
  }

  join: state_staff_supervisor_period {
    sql_on: ${state_staff.staff_id} = ${state_staff_supervisor_period.staff_id};;
    relationship: one_to_many
  }

}
explore: state_staff_caseload_type_period {
  extension: required

}
explore: state_staff_external_id {
  extension: required

}
explore: state_staff_location_period {
  extension: required

}
explore: state_staff_role_period {
  extension: required

}
explore: state_staff_supervisor_period {
  extension: required

}
