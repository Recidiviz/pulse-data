# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

- dashboard: normalized_state_staff
  layout: newspaper
  title: Normalized State Staff
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
    explore: normalized_state_staff
    field: normalized_state_staff.staff_id

  - name: State Code
    title: State Code
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: normalized_state_staff
    field: normalized_state_staff.state_code

  - name: External Id
    title: External Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: normalized_state_staff
    field: normalized_state_staff_external_id.external_id

  - name: Id Type
    title: Id Type
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: normalized_state_staff
    field: normalized_state_staff_external_id.id_type

  elements:
  - name: Normalized State Staff
    title: Normalized State Staff
    explore: normalized_state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [normalized_state_staff.email,
      normalized_state_staff.full_name,
      normalized_state_staff.staff_id,
      normalized_state_staff.state_code]
    sorts: []
    listen: 
      Staff Id: normalized_state_staff.staff_id
      State Code: normalized_state_staff.state_code
      External Id: normalized_state_staff_external_id.external_id
      Id Type: normalized_state_staff_external_id.id_type
    row: 0
    col: 0
    width: 12
    height: 6

  - name: Normalized State Staff Caseload Type Period
    title: Normalized State Staff Caseload Type Period
    explore: normalized_state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [normalized_state_staff_caseload_type_period.caseload_type,
      normalized_state_staff_caseload_type_period.caseload_type_raw_text,
      normalized_state_staff_caseload_type_period.end_date,
      normalized_state_staff_caseload_type_period.external_id,
      normalized_state_staff_caseload_type_period.staff_caseload_type_period_id,
      normalized_state_staff_caseload_type_period.start_date,
      normalized_state_staff_caseload_type_period.state_code]
    sorts: [normalized_state_staff_caseload_type_period.end_date desc, normalized_state_staff_caseload_type_period.start_date desc]
    listen: 
      Staff Id: normalized_state_staff.staff_id
      State Code: normalized_state_staff.state_code
      External Id: normalized_state_staff_external_id.external_id
      Id Type: normalized_state_staff_external_id.id_type
    row: 0
    col: 12
    width: 12
    height: 6

  - name: Normalized State Staff External Id
    title: Normalized State Staff External Id
    explore: normalized_state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [normalized_state_staff_external_id.external_id,
      normalized_state_staff_external_id.id_type,
      normalized_state_staff_external_id.staff_external_id_id,
      normalized_state_staff_external_id.state_code]
    sorts: []
    listen: 
      Staff Id: normalized_state_staff.staff_id
      State Code: normalized_state_staff.state_code
      External Id: normalized_state_staff_external_id.external_id
      Id Type: normalized_state_staff_external_id.id_type
    row: 6
    col: 0
    width: 12
    height: 6

  - name: Normalized State Staff Location Period
    title: Normalized State Staff Location Period
    explore: normalized_state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [normalized_state_staff_location_period.end_date,
      normalized_state_staff_location_period.external_id,
      normalized_state_staff_location_period.location_external_id,
      normalized_state_staff_location_period.staff_location_period_id,
      normalized_state_staff_location_period.start_date,
      normalized_state_staff_location_period.state_code]
    sorts: [normalized_state_staff_location_period.end_date desc, normalized_state_staff_location_period.start_date desc]
    listen: 
      Staff Id: normalized_state_staff.staff_id
      State Code: normalized_state_staff.state_code
      External Id: normalized_state_staff_external_id.external_id
      Id Type: normalized_state_staff_external_id.id_type
    row: 6
    col: 12
    width: 12
    height: 6

  - name: Normalized State Staff Role Period
    title: Normalized State Staff Role Period
    explore: normalized_state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [normalized_state_staff_role_period.end_date,
      normalized_state_staff_role_period.external_id,
      normalized_state_staff_role_period.role_subtype,
      normalized_state_staff_role_period.role_subtype_raw_text,
      normalized_state_staff_role_period.role_type,
      normalized_state_staff_role_period.role_type_raw_text,
      normalized_state_staff_role_period.staff_role_period_id,
      normalized_state_staff_role_period.start_date,
      normalized_state_staff_role_period.state_code]
    sorts: [normalized_state_staff_role_period.end_date desc, normalized_state_staff_role_period.start_date desc]
    listen: 
      Staff Id: normalized_state_staff.staff_id
      State Code: normalized_state_staff.state_code
      External Id: normalized_state_staff_external_id.external_id
      Id Type: normalized_state_staff_external_id.id_type
    row: 12
    col: 0
    width: 12
    height: 6

  - name: Normalized State Staff Supervisor Period
    title: Normalized State Staff Supervisor Period
    explore: normalized_state_staff
    model: "@{model_name}"
    type: looker_grid
    fields: [normalized_state_staff_supervisor_period.end_date,
      normalized_state_staff_supervisor_period.external_id,
      normalized_state_staff_supervisor_period.staff_supervisor_period_id,
      normalized_state_staff_supervisor_period.start_date,
      normalized_state_staff_supervisor_period.state_code,
      normalized_state_staff_supervisor_period.supervisor_staff_external_id,
      normalized_state_staff_supervisor_period.supervisor_staff_external_id_type,
      normalized_state_staff_supervisor_period.supervisor_staff_id]
    sorts: [normalized_state_staff_supervisor_period.end_date desc, normalized_state_staff_supervisor_period.start_date desc]
    listen: 
      Staff Id: normalized_state_staff.staff_id
      State Code: normalized_state_staff.state_code
      External Id: normalized_state_staff_external_id.external_id
      Id Type: normalized_state_staff_external_id.id_type
    row: 12
    col: 12
    width: 12
    height: 6

