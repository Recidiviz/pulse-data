# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

- dashboard: state_person
  layout: newspaper
  title: State Person
  load_configuration: wait

  filters:
  - name: Person Id
    title: Person Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_person
    field: state_person.person_id

  - name: State Code
    title: State Code
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_person
    field: state_person.state_code

  - name: External Id
    title: External Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_person
    field: state_person_external_id.external_id

  - name: Id Type
    title: Id Type
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{model_name}"
    explore: state_person
    field: state_person_external_id.id_type

  elements:
  - name: info
    title: Info
    type: text
    body_text: __Info:__ ❇️ - Indicates an open incarceration/supervision period in respective tables. __Note:__ Person_id is not consistent between production and staging environments, make sure to filter by external id and state if you want to compare a person between the two environments.
    row: 0
    col: 0
    width: 24
    height: 2

  - name: actions
    title: Actions
    explore: state_person
    model: "@{model_name}"
    type: single_value
    fields: [state_person.actions]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 2
    col: 0
    width: 24
    height: 2

  - name: periods_timeline
    title: Periods Timeline
    explore: state_person
    model: "@{model_name}"
    type: looker_timeline
    fields: [state_person.person_id,
      state_person_periods.period_type,
      state_person_periods.start_date,
      state_person_periods.end_date]
    sorts: [state_person_periods.start_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    show_legend: True
    group_bars: False
    color_application: 
      collection_id: recidiviz-color-collection
      palette_id: recidiviz-color-collection-categorical-0
    row: 8
    col: 0
    width: 24
    height: 6

  - name: State Assessment
    title: State Assessment
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_assessment.assessment_class,
      state_assessment.assessment_class_raw_text,
      state_assessment.assessment_date,
      state_assessment.assessment_id,
      state_assessment.assessment_level,
      state_assessment.assessment_level_raw_text,
      state_assessment.assessment_metadata,
      state_assessment.assessment_score,
      state_assessment.assessment_type,
      state_assessment.assessment_type_raw_text,
      state_assessment.conducting_staff_external_id,
      state_assessment.conducting_staff_external_id_type,
      state_assessment.external_id,
      state_assessment.state_code]
    sorts: [state_assessment.assessment_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 14
    col: 0
    width: 12
    height: 6

  - name: State Charge
    title: State Charge
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_charge.attempted,
      state_charge.charge_id,
      state_charge.charge_notes,
      state_charge.charging_entity,
      state_charge.classification_subtype,
      state_charge.classification_type,
      state_charge.classification_type_raw_text,
      state_charge.counts,
      state_charge.county_code,
      state_charge.date_charged_date,
      state_charge.description,
      state_charge.external_id,
      state_charge.is_controlling,
      state_charge.is_drug,
      state_charge.is_sex_offense,
      state_charge.is_violent,
      state_charge.judge_external_id,
      state_charge.judge_full_name,
      state_charge.judicial_district_code,
      state_charge.ncic_code,
      state_charge.offense_date,
      state_charge.offense_type,
      state_charge.state_code,
      state_charge.status,
      state_charge.status_raw_text,
      state_charge.statute]
    sorts: [state_charge.date_charged_date desc, state_charge.offense_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 14
    col: 12
    width: 12
    height: 6

  - name: State Charge V2
    title: State Charge V2
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_charge_v2.attempted,
      state_charge_v2.charge_notes,
      state_charge_v2.charge_v2_id,
      state_charge_v2.charging_entity,
      state_charge_v2.classification_subtype,
      state_charge_v2.classification_type,
      state_charge_v2.classification_type_raw_text,
      state_charge_v2.counts,
      state_charge_v2.county_code,
      state_charge_v2.date_charged_date,
      state_charge_v2.description,
      state_charge_v2.external_id,
      state_charge_v2.is_controlling,
      state_charge_v2.is_drug,
      state_charge_v2.is_sex_offense,
      state_charge_v2.is_violent,
      state_charge_v2.judge_external_id,
      state_charge_v2.judge_full_name,
      state_charge_v2.judicial_district_code,
      state_charge_v2.ncic_code,
      state_charge_v2.offense_date,
      state_charge_v2.offense_type,
      state_charge_v2.state_code,
      state_charge_v2.status,
      state_charge_v2.status_raw_text,
      state_charge_v2.statute]
    sorts: [state_charge_v2.date_charged_date desc, state_charge_v2.offense_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 20
    col: 0
    width: 12
    height: 6

  - name: State Drug Screen
    title: State Drug Screen
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_drug_screen.drug_screen_date,
      state_drug_screen.drug_screen_id,
      state_drug_screen.drug_screen_metadata,
      state_drug_screen.drug_screen_result,
      state_drug_screen.drug_screen_result_raw_text,
      state_drug_screen.external_id,
      state_drug_screen.sample_type,
      state_drug_screen.sample_type_raw_text,
      state_drug_screen.state_code]
    sorts: [state_drug_screen.drug_screen_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 20
    col: 12
    width: 12
    height: 6

  - name: State Early Discharge
    title: State Early Discharge
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_early_discharge.county_code,
      state_early_discharge.deciding_body_type,
      state_early_discharge.deciding_body_type_raw_text,
      state_early_discharge.decision,
      state_early_discharge.decision_date,
      state_early_discharge.decision_raw_text,
      state_early_discharge.decision_status,
      state_early_discharge.decision_status_raw_text,
      state_early_discharge.early_discharge_id,
      state_early_discharge.external_id,
      state_early_discharge.incarceration_sentence_id,
      state_early_discharge.request_date,
      state_early_discharge.requesting_body_type,
      state_early_discharge.requesting_body_type_raw_text,
      state_early_discharge.state_code,
      state_early_discharge.supervision_sentence_id]
    sorts: [state_early_discharge.decision_date desc, state_early_discharge.request_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 26
    col: 0
    width: 12
    height: 6

  - name: State Employment Period
    title: State Employment Period
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_employment_period.employer_address,
      state_employment_period.employer_name,
      state_employment_period.employment_period_id,
      state_employment_period.employment_status,
      state_employment_period.employment_status_raw_text,
      state_employment_period.end_date,
      state_employment_period.end_reason,
      state_employment_period.end_reason_raw_text,
      state_employment_period.external_id,
      state_employment_period.job_title,
      state_employment_period.last_verified_date,
      state_employment_period.start_date,
      state_employment_period.state_code]
    sorts: [state_employment_period.end_date desc, state_employment_period.last_verified_date desc, state_employment_period.start_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 26
    col: 12
    width: 12
    height: 6

  - name: State Incarceration Incident
    title: State Incarceration Incident
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_incarceration_incident.external_id,
      state_incarceration_incident.facility,
      state_incarceration_incident.incarceration_incident_id,
      state_incarceration_incident.incident_date,
      state_incarceration_incident.incident_details,
      state_incarceration_incident.incident_metadata,
      state_incarceration_incident.incident_severity,
      state_incarceration_incident.incident_severity_raw_text,
      state_incarceration_incident.incident_type,
      state_incarceration_incident.incident_type_raw_text,
      state_incarceration_incident.location_within_facility,
      state_incarceration_incident.state_code]
    sorts: [state_incarceration_incident.incident_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 32
    col: 0
    width: 12
    height: 6

  - name: State Incarceration Incident Outcome
    title: State Incarceration Incident Outcome
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_incarceration_incident_outcome.date_effective_date,
      state_incarceration_incident_outcome.external_id,
      state_incarceration_incident_outcome.hearing_date,
      state_incarceration_incident_outcome.incarceration_incident_id,
      state_incarceration_incident_outcome.incarceration_incident_outcome_id,
      state_incarceration_incident_outcome.outcome_description,
      state_incarceration_incident_outcome.outcome_metadata,
      state_incarceration_incident_outcome.outcome_type,
      state_incarceration_incident_outcome.outcome_type_raw_text,
      state_incarceration_incident_outcome.projected_end_date,
      state_incarceration_incident_outcome.punishment_length_days,
      state_incarceration_incident_outcome.report_date,
      state_incarceration_incident_outcome.state_code]
    sorts: [state_incarceration_incident_outcome.date_effective_date desc, state_incarceration_incident_outcome.hearing_date desc, state_incarceration_incident_outcome.projected_end_date desc, state_incarceration_incident_outcome.report_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 32
    col: 12
    width: 12
    height: 6

  - name: State Incarceration Period
    title: State Incarceration Period
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_incarceration_period.admission_date,
      state_incarceration_period.admission_reason,
      state_incarceration_period.admission_reason_raw_text,
      state_incarceration_period.county_code,
      state_incarceration_period.custodial_authority,
      state_incarceration_period.custodial_authority_raw_text,
      state_incarceration_period.custody_level,
      state_incarceration_period.custody_level_raw_text,
      state_incarceration_period.external_id,
      state_incarceration_period.facility,
      state_incarceration_period.housing_unit,
      state_incarceration_period.housing_unit_category,
      state_incarceration_period.housing_unit_category_raw_text,
      state_incarceration_period.housing_unit_type,
      state_incarceration_period.housing_unit_type_raw_text,
      state_incarceration_period.incarceration_period_id,
      state_incarceration_period.incarceration_type,
      state_incarceration_period.incarceration_type_raw_text,
      state_incarceration_period.person_id,
      state_incarceration_period.release_date,
      state_incarceration_period.release_reason,
      state_incarceration_period.release_reason_raw_text,
      state_incarceration_period.specialized_purpose_for_incarceration,
      state_incarceration_period.specialized_purpose_for_incarceration_raw_text,
      state_incarceration_period.state_code]
    sorts: [state_incarceration_period.admission_date desc, state_incarceration_period.release_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 38
    col: 0
    width: 12
    height: 6

  - name: State Incarceration Sentence
    title: State Incarceration Sentence
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_incarceration_sentence.completion_date,
      state_incarceration_sentence.conditions,
      state_incarceration_sentence.county_code,
      state_incarceration_sentence.date_imposed_date,
      state_incarceration_sentence.earned_time_days,
      state_incarceration_sentence.effective_date,
      state_incarceration_sentence.external_id,
      state_incarceration_sentence.good_time_days,
      state_incarceration_sentence.incarceration_sentence_id,
      state_incarceration_sentence.incarceration_type,
      state_incarceration_sentence.incarceration_type_raw_text,
      state_incarceration_sentence.initial_time_served_days,
      state_incarceration_sentence.is_capital_punishment,
      state_incarceration_sentence.is_life,
      state_incarceration_sentence.max_length_days,
      state_incarceration_sentence.min_length_days,
      state_incarceration_sentence.parole_eligibility_date,
      state_incarceration_sentence.parole_possible,
      state_incarceration_sentence.projected_max_release_date,
      state_incarceration_sentence.projected_min_release_date,
      state_incarceration_sentence.sentence_metadata,
      state_incarceration_sentence.state_code,
      state_incarceration_sentence.status,
      state_incarceration_sentence.status_raw_text]
    sorts: [state_incarceration_sentence.completion_date desc, state_incarceration_sentence.date_imposed_date desc, state_incarceration_sentence.effective_date desc, state_incarceration_sentence.parole_eligibility_date desc, state_incarceration_sentence.projected_max_release_date desc, state_incarceration_sentence.projected_min_release_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 38
    col: 12
    width: 12
    height: 6

  - name: State Person
    title: State Person
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_person.birthdate_date,
      state_person.current_address,
      state_person.current_email_address,
      state_person.current_phone_number,
      state_person.full_name,
      state_person.full_name_clean,
      state_person.gender,
      state_person.gender_raw_text,
      state_person.person_id,
      state_person.residency_status,
      state_person.residency_status_raw_text,
      state_person.sex,
      state_person.sex_raw_text,
      state_person.state_code,
      state_person_ethnicity.count,
      state_person_race.count]
    sorts: [state_person.birthdate_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 44
    col: 0
    width: 12
    height: 6

  - name: State Person Address Period
    title: State Person Address Period
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_person_address_period.address_city,
      state_person_address_period.address_country,
      state_person_address_period.address_county,
      state_person_address_period.address_end_date,
      state_person_address_period.address_is_verified,
      state_person_address_period.address_line_1,
      state_person_address_period.address_line_2,
      state_person_address_period.address_metadata,
      state_person_address_period.address_start_date,
      state_person_address_period.address_state,
      state_person_address_period.address_type,
      state_person_address_period.address_type_raw_text,
      state_person_address_period.address_zip,
      state_person_address_period.full_address,
      state_person_address_period.person_address_period_id,
      state_person_address_period.state_code]
    sorts: [state_person_address_period.address_end_date desc, state_person_address_period.address_start_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 44
    col: 12
    width: 12
    height: 6

  - name: State Person Alias
    title: State Person Alias
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_person_alias.alias_type,
      state_person_alias.alias_type_raw_text,
      state_person_alias.full_name,
      state_person_alias.person_alias_id,
      state_person_alias.state_code]
    sorts: []
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 50
    col: 0
    width: 12
    height: 6

  - name: State Person External Id
    title: State Person External Id
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_person_external_id.external_id,
      state_person_external_id.external_id_with_type,
      state_person_external_id.id_active_from_datetime_date,
      state_person_external_id.id_active_to_datetime_date,
      state_person_external_id.id_type,
      state_person_external_id.is_current_display_id_for_type,
      state_person_external_id.is_stable_id_for_type,
      state_person_external_id.person_external_id_id,
      state_person_external_id.state_code]
    sorts: [state_person_external_id.id_active_from_datetime_date desc, state_person_external_id.id_active_to_datetime_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 50
    col: 12
    width: 12
    height: 6

  - name: State Person Housing Status Period
    title: State Person Housing Status Period
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_person_housing_status_period.housing_status_end_date,
      state_person_housing_status_period.housing_status_start_date,
      state_person_housing_status_period.housing_status_type,
      state_person_housing_status_period.housing_status_type_raw_text,
      state_person_housing_status_period.person_housing_status_period_id,
      state_person_housing_status_period.state_code]
    sorts: [state_person_housing_status_period.housing_status_end_date desc, state_person_housing_status_period.housing_status_start_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 56
    col: 0
    width: 12
    height: 6

  - name: State Person Staff Relationship Period
    title: State Person Staff Relationship Period
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_person_staff_relationship_period.associated_staff_external_id,
      state_person_staff_relationship_period.associated_staff_external_id_type,
      state_person_staff_relationship_period.location_external_id,
      state_person_staff_relationship_period.person_staff_relationship_period_id,
      state_person_staff_relationship_period.relationship_end_date_exclusive_date,
      state_person_staff_relationship_period.relationship_priority,
      state_person_staff_relationship_period.relationship_start_date,
      state_person_staff_relationship_period.relationship_type,
      state_person_staff_relationship_period.relationship_type_raw_text,
      state_person_staff_relationship_period.state_code,
      state_person_staff_relationship_period.system_type,
      state_person_staff_relationship_period.system_type_raw_text]
    sorts: [state_person_staff_relationship_period.relationship_end_date_exclusive_date desc, state_person_staff_relationship_period.relationship_start_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 56
    col: 12
    width: 12
    height: 6

  - name: State Program Assignment
    title: State Program Assignment
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_program_assignment.discharge_date,
      state_program_assignment.external_id,
      state_program_assignment.participation_status,
      state_program_assignment.participation_status_raw_text,
      state_program_assignment.program_assignment_id,
      state_program_assignment.program_id,
      state_program_assignment.program_location_id,
      state_program_assignment.referral_date,
      state_program_assignment.referral_metadata,
      state_program_assignment.referring_staff_external_id,
      state_program_assignment.referring_staff_external_id_type,
      state_program_assignment.start_date,
      state_program_assignment.state_code]
    sorts: [state_program_assignment.discharge_date desc, state_program_assignment.referral_date desc, state_program_assignment.start_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 62
    col: 0
    width: 12
    height: 6

  - name: State Scheduled Supervision Contact
    title: State Scheduled Supervision Contact
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_scheduled_supervision_contact.contact_meeting_address,
      state_scheduled_supervision_contact.contact_method,
      state_scheduled_supervision_contact.contact_method_raw_text,
      state_scheduled_supervision_contact.contact_reason,
      state_scheduled_supervision_contact.contact_reason_raw_text,
      state_scheduled_supervision_contact.contact_type,
      state_scheduled_supervision_contact.contact_type_raw_text,
      state_scheduled_supervision_contact.contacting_staff_external_id,
      state_scheduled_supervision_contact.contacting_staff_external_id_type,
      state_scheduled_supervision_contact.external_id,
      state_scheduled_supervision_contact.location,
      state_scheduled_supervision_contact.location_raw_text,
      state_scheduled_supervision_contact.scheduled_contact_date,
      state_scheduled_supervision_contact.scheduled_contact_datetime_date,
      state_scheduled_supervision_contact.scheduled_supervision_contact_id,
      state_scheduled_supervision_contact.scheduled_supervision_contact_metadata,
      state_scheduled_supervision_contact.sequence_num,
      state_scheduled_supervision_contact.state_code,
      state_scheduled_supervision_contact.status,
      state_scheduled_supervision_contact.status_raw_text,
      state_scheduled_supervision_contact.update_datetime_date]
    sorts: [state_scheduled_supervision_contact.scheduled_contact_date desc, state_scheduled_supervision_contact.scheduled_contact_datetime_date desc, state_scheduled_supervision_contact.update_datetime_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 62
    col: 12
    width: 12
    height: 6

  - name: State Sentence
    title: State Sentence
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_sentence.conditions,
      state_sentence.county_code,
      state_sentence.current_state_provided_start_date,
      state_sentence.external_id,
      state_sentence.imposed_date,
      state_sentence.initial_time_served_days,
      state_sentence.is_capital_punishment,
      state_sentence.is_life,
      state_sentence.parent_sentence_external_id_array,
      state_sentence.parole_possible,
      state_sentence.sentence_group_external_id,
      state_sentence.sentence_id,
      state_sentence.sentence_metadata,
      state_sentence.sentence_type,
      state_sentence.sentence_type_raw_text,
      state_sentence.sentencing_authority,
      state_sentence.sentencing_authority_raw_text,
      state_sentence.state_code]
    sorts: [state_sentence.current_state_provided_start_date desc, state_sentence.imposed_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 68
    col: 0
    width: 12
    height: 6

  - name: State Sentence Group
    title: State Sentence Group
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_sentence_group.external_id,
      state_sentence_group.sentence_group_id,
      state_sentence_group.state_code]
    sorts: []
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 68
    col: 12
    width: 12
    height: 6

  - name: State Sentence Group Length
    title: State Sentence Group Length
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_sentence_group_length.group_update_datetime_date,
      state_sentence_group_length.parole_eligibility_date_external_date,
      state_sentence_group_length.projected_full_term_release_date_max_external_date,
      state_sentence_group_length.projected_full_term_release_date_min_external_date,
      state_sentence_group_length.projected_parole_release_date_external_date,
      state_sentence_group_length.sentence_group_id,
      state_sentence_group_length.sentence_group_length_id,
      state_sentence_group_length.sentence_group_length_metadata,
      state_sentence_group_length.sequence_num,
      state_sentence_group_length.state_code]
    sorts: [state_sentence_group_length.group_update_datetime_date desc, state_sentence_group_length.parole_eligibility_date_external_date desc, state_sentence_group_length.projected_full_term_release_date_max_external_date desc, state_sentence_group_length.projected_full_term_release_date_min_external_date desc, state_sentence_group_length.projected_parole_release_date_external_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 74
    col: 0
    width: 12
    height: 6

  - name: State Sentence Length
    title: State Sentence Length
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_sentence_length.earned_time_days,
      state_sentence_length.good_time_days,
      state_sentence_length.length_update_datetime_date,
      state_sentence_length.parole_eligibility_date_external_date,
      state_sentence_length.projected_completion_date_max_external_date,
      state_sentence_length.projected_completion_date_min_external_date,
      state_sentence_length.projected_parole_release_date_external_date,
      state_sentence_length.sentence_id,
      state_sentence_length.sentence_length_days_max,
      state_sentence_length.sentence_length_days_min,
      state_sentence_length.sentence_length_id,
      state_sentence_length.sequence_num,
      state_sentence_length.state_code]
    sorts: [state_sentence_length.length_update_datetime_date desc, state_sentence_length.parole_eligibility_date_external_date desc, state_sentence_length.projected_completion_date_max_external_date desc, state_sentence_length.projected_completion_date_min_external_date desc, state_sentence_length.projected_parole_release_date_external_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 74
    col: 12
    width: 12
    height: 6

  - name: State Sentence Status Snapshot
    title: State Sentence Status Snapshot
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_sentence_status_snapshot.sentence_id,
      state_sentence_status_snapshot.sentence_status_snapshot_id,
      state_sentence_status_snapshot.sequence_num,
      state_sentence_status_snapshot.state_code,
      state_sentence_status_snapshot.status,
      state_sentence_status_snapshot.status_raw_text,
      state_sentence_status_snapshot.status_update_datetime_date]
    sorts: [state_sentence_status_snapshot.status_update_datetime_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 80
    col: 0
    width: 12
    height: 6

  - name: State Supervision Contact
    title: State Supervision Contact
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_supervision_contact.contact_date,
      state_supervision_contact.contact_datetime_date,
      state_supervision_contact.contact_method,
      state_supervision_contact.contact_method_raw_text,
      state_supervision_contact.contact_reason,
      state_supervision_contact.contact_reason_raw_text,
      state_supervision_contact.contact_type,
      state_supervision_contact.contact_type_raw_text,
      state_supervision_contact.contacting_staff_external_id,
      state_supervision_contact.contacting_staff_external_id_type,
      state_supervision_contact.external_id,
      state_supervision_contact.location,
      state_supervision_contact.location_raw_text,
      state_supervision_contact.resulted_in_arrest,
      state_supervision_contact.scheduled_contact_date,
      state_supervision_contact.scheduled_contact_datetime_date,
      state_supervision_contact.state_code,
      state_supervision_contact.status,
      state_supervision_contact.status_raw_text,
      state_supervision_contact.supervision_contact_id,
      state_supervision_contact.supervision_contact_metadata,
      state_supervision_contact.verified_employment]
    sorts: [state_supervision_contact.contact_date desc, state_supervision_contact.contact_datetime_date desc, state_supervision_contact.scheduled_contact_date desc, state_supervision_contact.scheduled_contact_datetime_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 80
    col: 12
    width: 12
    height: 6

  - name: State Supervision Period
    title: State Supervision Period
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_supervision_case_type_entry.count,
      state_supervision_period.admission_reason,
      state_supervision_period.admission_reason_raw_text,
      state_supervision_period.conditions,
      state_supervision_period.county_code,
      state_supervision_period.custodial_authority,
      state_supervision_period.custodial_authority_raw_text,
      state_supervision_period.external_id,
      state_supervision_period.person_id,
      state_supervision_period.start_date,
      state_supervision_period.state_code,
      state_supervision_period.supervising_officer_staff_external_id,
      state_supervision_period.supervising_officer_staff_external_id_type,
      state_supervision_period.supervision_level,
      state_supervision_period.supervision_level_raw_text,
      state_supervision_period.supervision_period_id,
      state_supervision_period.supervision_period_metadata,
      state_supervision_period.supervision_site,
      state_supervision_period.supervision_type,
      state_supervision_period.supervision_type_raw_text,
      state_supervision_period.termination_date,
      state_supervision_period.termination_reason,
      state_supervision_period.termination_reason_raw_text]
    sorts: [state_supervision_period.start_date desc, state_supervision_period.termination_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 86
    col: 0
    width: 12
    height: 6

  - name: State Supervision Sentence
    title: State Supervision Sentence
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_supervision_sentence.completion_date,
      state_supervision_sentence.conditions,
      state_supervision_sentence.county_code,
      state_supervision_sentence.date_imposed_date,
      state_supervision_sentence.effective_date,
      state_supervision_sentence.external_id,
      state_supervision_sentence.is_life,
      state_supervision_sentence.max_length_days,
      state_supervision_sentence.min_length_days,
      state_supervision_sentence.projected_completion_date,
      state_supervision_sentence.sentence_metadata,
      state_supervision_sentence.state_code,
      state_supervision_sentence.status,
      state_supervision_sentence.status_raw_text,
      state_supervision_sentence.supervision_sentence_id,
      state_supervision_sentence.supervision_type,
      state_supervision_sentence.supervision_type_raw_text]
    sorts: [state_supervision_sentence.completion_date desc, state_supervision_sentence.date_imposed_date desc, state_supervision_sentence.effective_date desc, state_supervision_sentence.projected_completion_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 86
    col: 12
    width: 12
    height: 6

  - name: State Supervision Violation
    title: State Supervision Violation
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_supervision_violated_condition_entry.count,
      state_supervision_violation.external_id,
      state_supervision_violation.is_sex_offense,
      state_supervision_violation.is_violent,
      state_supervision_violation.state_code,
      state_supervision_violation.supervision_violation_id,
      state_supervision_violation.violation_date,
      state_supervision_violation.violation_metadata,
      state_supervision_violation.violation_severity,
      state_supervision_violation.violation_severity_raw_text,
      state_supervision_violation_type_entry.count]
    sorts: [state_supervision_violation.violation_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 92
    col: 0
    width: 12
    height: 6

  - name: State Supervision Violation Response
    title: State Supervision Violation Response
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_supervision_violation_response.deciding_body_type,
      state_supervision_violation_response.deciding_body_type_raw_text,
      state_supervision_violation_response.deciding_staff_external_id,
      state_supervision_violation_response.deciding_staff_external_id_type,
      state_supervision_violation_response.external_id,
      state_supervision_violation_response.is_draft,
      state_supervision_violation_response.response_date,
      state_supervision_violation_response.response_subtype,
      state_supervision_violation_response.response_type,
      state_supervision_violation_response.response_type_raw_text,
      state_supervision_violation_response.state_code,
      state_supervision_violation_response.supervision_violation_id,
      state_supervision_violation_response.supervision_violation_response_id,
      state_supervision_violation_response.violation_response_metadata,
      state_supervision_violation_response.violation_response_severity,
      state_supervision_violation_response.violation_response_severity_raw_text,
      state_supervision_violation_response_decision_entry.count]
    sorts: [state_supervision_violation_response.response_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 92
    col: 12
    width: 12
    height: 6

  - name: State Task Deadline
    title: State Task Deadline
    explore: state_person
    model: "@{model_name}"
    type: looker_grid
    fields: [state_task_deadline.due_date,
      state_task_deadline.eligible_date,
      state_task_deadline.sequence_num,
      state_task_deadline.state_code,
      state_task_deadline.task_deadline_id,
      state_task_deadline.task_metadata,
      state_task_deadline.task_subtype,
      state_task_deadline.task_type,
      state_task_deadline.task_type_raw_text,
      state_task_deadline.update_datetime_date]
    sorts: [state_task_deadline.due_date desc, state_task_deadline.eligible_date desc, state_task_deadline.update_datetime_date desc]
    listen: 
      Person Id: state_person.person_id
      State Code: state_person.state_code
      External Id: state_person_external_id.external_id
      Id Type: state_person_external_id.id_type
    row: 98
    col: 0
    width: 12
    height: 6

