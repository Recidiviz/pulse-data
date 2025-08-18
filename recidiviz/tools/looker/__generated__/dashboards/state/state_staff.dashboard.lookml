# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

- dashboard: state_staff
  layout: newspaper
  title: State Staff
  load_configuration: wait

  filters:
  - name: Staff Id
    title: Staff Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_staff
    field: state_staff.staff_id

  - name: State Code
    title: State Code
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_staff
    field: state_staff.state_code

  - name: External Id
    title: External Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_staff
    field: state_staff_external_id.external_id

  - name: Id Type
    title: Id Type
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_staff
    field: state_staff_external_id.id_type

  elements:
  - name: State Staff
    title: State Staff
    explore: state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [state_staff.email,
      state_staff.full_name,
      state_staff.staff_id,
      state_staff.state_code]
    sorts: []
    listen: 
      Staff Id: state_staff.staff_id
      State Code: state_staff.state_code
      External Id: state_staff_external_id.external_id
      Id Type: state_staff_external_id.id_type
    row: 0
    col: 0
    width: 12
    height: 6

  - name: State Staff Caseload Type Period
    title: State Staff Caseload Type Period
    explore: state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [state_staff_caseload_type_period.caseload_type,
      state_staff_caseload_type_period.caseload_type_raw_text,
      state_staff_caseload_type_period.end_date,
      state_staff_caseload_type_period.external_id,
      state_staff_caseload_type_period.staff_caseload_type_period_id,
      state_staff_caseload_type_period.start_date,
      state_staff_caseload_type_period.state_code]
    sorts: [state_staff_caseload_type_period.end_date desc, state_staff_caseload_type_period.start_date desc]
    listen: 
      Staff Id: state_staff.staff_id
      State Code: state_staff.state_code
      External Id: state_staff_external_id.external_id
      Id Type: state_staff_external_id.id_type
    row: 0
    col: 12
    width: 12
    height: 6

  - name: State Staff External Id
    title: State Staff External Id
    explore: state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [state_staff_external_id.external_id,
      state_staff_external_id.id_type,
      state_staff_external_id.staff_external_id_id,
      state_staff_external_id.state_code]
    sorts: []
    listen: 
      Staff Id: state_staff.staff_id
      State Code: state_staff.state_code
      External Id: state_staff_external_id.external_id
      Id Type: state_staff_external_id.id_type
    row: 6
    col: 0
    width: 12
    height: 6

  - name: State Staff Location Period
    title: State Staff Location Period
    explore: state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [state_staff_location_period.end_date,
      state_staff_location_period.external_id,
      state_staff_location_period.location_external_id,
      state_staff_location_period.staff_location_period_id,
      state_staff_location_period.start_date,
      state_staff_location_period.state_code]
    sorts: [state_staff_location_period.end_date desc, state_staff_location_period.start_date desc]
    listen: 
      Staff Id: state_staff.staff_id
      State Code: state_staff.state_code
      External Id: state_staff_external_id.external_id
      Id Type: state_staff_external_id.id_type
    row: 6
    col: 12
    width: 12
    height: 6

  - name: State Staff Role Period
    title: State Staff Role Period
    explore: state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [state_staff_role_period.end_date,
      state_staff_role_period.external_id,
      state_staff_role_period.role_subtype,
      state_staff_role_period.role_subtype_raw_text,
      state_staff_role_period.role_type,
      state_staff_role_period.role_type_raw_text,
      state_staff_role_period.staff_role_period_id,
      state_staff_role_period.start_date,
      state_staff_role_period.state_code]
    sorts: [state_staff_role_period.end_date desc, state_staff_role_period.start_date desc]
    listen: 
      Staff Id: state_staff.staff_id
      State Code: state_staff.state_code
      External Id: state_staff_external_id.external_id
      Id Type: state_staff_external_id.id_type
    row: 12
    col: 0
    width: 12
    height: 6

  - name: State Staff Supervisor Period
    title: State Staff Supervisor Period
    explore: state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [state_staff_supervisor_period.end_date,
      state_staff_supervisor_period.external_id,
      state_staff_supervisor_period.staff_supervisor_period_id,
      state_staff_supervisor_period.start_date,
      state_staff_supervisor_period.state_code,
      state_staff_supervisor_period.supervisor_staff_external_id,
      state_staff_supervisor_period.supervisor_staff_external_id_type]
    sorts: [state_staff_supervisor_period.end_date desc, state_staff_supervisor_period.start_date desc]
    listen: 
      Staff Id: state_staff.staff_id
      State Code: state_staff.state_code
      External Id: state_staff_external_id.external_id
      Id Type: state_staff_external_id.id_type
    row: 12
    col: 12
    width: 12
    height: 6

