# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/state_dataset_lookml_generator.py`.

- dashboard: normalized_state_person
  layout: newspaper
  title: Normalized State Person
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
    model: recidiviz-staging
    explore: normalized_state_person
    field: normalized_state_person.person_id

  - name: State Code
    title: State Code
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: recidiviz-staging
    explore: normalized_state_person
    field: normalized_state_person.state_code

  - name: External Id
    title: External Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: recidiviz-staging
    explore: normalized_state_person
    field: normalized_state_person_external_id.external_id

  - name: Id Type
    title: Id Type
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: recidiviz-staging
    explore: normalized_state_person
    field: normalized_state_person_external_id.id_type

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
    explore: normalized_state_person
    model: recidiviz-staging
    type: single_value
    fields: [normalized_state_person.actions]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 2
    col: 0
    width: 24
    height: 2

  - name: periods_timeline
    title: Periods Timeline
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_timeline
    fields: [normalized_state_person.person_id,
      normalized_state_person_periods.period_type,
      normalized_state_person_periods.start_date,
      normalized_state_person_periods.end_date]
    sorts: [normalized_state_person_periods.start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    show_legend: True
    group_bars: False
    color_application: 
      collection_id: recidiviz-color-collection
      palette_id: recidiviz-color-collection-categorical-0
    row: 8
    col: 0
    width: 24
    height: 6

  - name: Normalized State Assessment
    title: Normalized State Assessment
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_assessment.assessment_class,
      normalized_state_assessment.assessment_class_raw_text,
      normalized_state_assessment.assessment_date,
      normalized_state_assessment.assessment_id,
      normalized_state_assessment.assessment_level,
      normalized_state_assessment.assessment_level_raw_text,
      normalized_state_assessment.assessment_metadata,
      normalized_state_assessment.assessment_score,
      normalized_state_assessment.assessment_score_bucket,
      normalized_state_assessment.assessment_type,
      normalized_state_assessment.assessment_type_raw_text,
      normalized_state_assessment.conducting_staff_external_id,
      normalized_state_assessment.conducting_staff_external_id_type,
      normalized_state_assessment.conducting_staff_id,
      normalized_state_assessment.external_id,
      normalized_state_assessment.sequence_num,
      normalized_state_assessment.state_code]
    sorts: [normalized_state_assessment.assessment_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 14
    col: 0
    width: 12
    height: 6

  - name: Normalized State Charge
    title: Normalized State Charge
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_charge.attempted,
      normalized_state_charge.charge_id,
      normalized_state_charge.charge_notes,
      normalized_state_charge.charging_entity,
      normalized_state_charge.classification_subtype,
      normalized_state_charge.classification_type,
      normalized_state_charge.classification_type_raw_text,
      normalized_state_charge.counts,
      normalized_state_charge.county_code,
      normalized_state_charge.date_charged_date,
      normalized_state_charge.description,
      normalized_state_charge.description_external,
      normalized_state_charge.external_id,
      normalized_state_charge.is_controlling,
      normalized_state_charge.is_drug_external,
      normalized_state_charge.is_sex_offense_external,
      normalized_state_charge.is_violent_external,
      normalized_state_charge.judge_external_id,
      normalized_state_charge.judge_full_name,
      normalized_state_charge.judicial_district_code,
      normalized_state_charge.ncic_category_external,
      normalized_state_charge.ncic_code_external,
      normalized_state_charge.offense_date,
      normalized_state_charge.offense_type,
      normalized_state_charge.state_code,
      normalized_state_charge.status,
      normalized_state_charge.status_raw_text,
      normalized_state_charge.statute]
    sorts: [normalized_state_charge.date_charged_date desc, normalized_state_charge.offense_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 14
    col: 12
    width: 12
    height: 6

  - name: Normalized State Charge V2
    title: Normalized State Charge V2
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_charge_v2.attempted,
      normalized_state_charge_v2.charge_notes,
      normalized_state_charge_v2.charge_v2_id,
      normalized_state_charge_v2.charging_entity,
      normalized_state_charge_v2.classification_subtype,
      normalized_state_charge_v2.classification_type,
      normalized_state_charge_v2.classification_type_raw_text,
      normalized_state_charge_v2.counts,
      normalized_state_charge_v2.county_code,
      normalized_state_charge_v2.date_charged_date,
      normalized_state_charge_v2.description,
      normalized_state_charge_v2.description_external,
      normalized_state_charge_v2.external_id,
      normalized_state_charge_v2.is_controlling,
      normalized_state_charge_v2.is_drug,
      normalized_state_charge_v2.is_drug_external,
      normalized_state_charge_v2.is_sex_offense,
      normalized_state_charge_v2.is_sex_offense_external,
      normalized_state_charge_v2.is_violent,
      normalized_state_charge_v2.is_violent_external,
      normalized_state_charge_v2.judge_external_id,
      normalized_state_charge_v2.judge_full_name,
      normalized_state_charge_v2.judicial_district_code,
      normalized_state_charge_v2.ncic_category_external,
      normalized_state_charge_v2.ncic_code,
      normalized_state_charge_v2.ncic_code_external,
      normalized_state_charge_v2.offense_date,
      normalized_state_charge_v2.offense_type,
      normalized_state_charge_v2.state_code,
      normalized_state_charge_v2.status,
      normalized_state_charge_v2.status_raw_text,
      normalized_state_charge_v2.statute]
    sorts: [normalized_state_charge_v2.date_charged_date desc, normalized_state_charge_v2.offense_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 20
    col: 0
    width: 12
    height: 6

  - name: Normalized State Drug Screen
    title: Normalized State Drug Screen
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_drug_screen.drug_screen_date,
      normalized_state_drug_screen.drug_screen_id,
      normalized_state_drug_screen.drug_screen_metadata,
      normalized_state_drug_screen.drug_screen_result,
      normalized_state_drug_screen.drug_screen_result_raw_text,
      normalized_state_drug_screen.external_id,
      normalized_state_drug_screen.sample_type,
      normalized_state_drug_screen.sample_type_raw_text,
      normalized_state_drug_screen.state_code]
    sorts: [normalized_state_drug_screen.drug_screen_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 20
    col: 12
    width: 12
    height: 6

  - name: Normalized State Early Discharge
    title: Normalized State Early Discharge
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_early_discharge.county_code,
      normalized_state_early_discharge.deciding_body_type,
      normalized_state_early_discharge.deciding_body_type_raw_text,
      normalized_state_early_discharge.decision,
      normalized_state_early_discharge.decision_date,
      normalized_state_early_discharge.decision_raw_text,
      normalized_state_early_discharge.decision_status,
      normalized_state_early_discharge.decision_status_raw_text,
      normalized_state_early_discharge.early_discharge_id,
      normalized_state_early_discharge.external_id,
      normalized_state_early_discharge.incarceration_sentence_id,
      normalized_state_early_discharge.request_date,
      normalized_state_early_discharge.requesting_body_type,
      normalized_state_early_discharge.requesting_body_type_raw_text,
      normalized_state_early_discharge.state_code,
      normalized_state_early_discharge.supervision_sentence_id]
    sorts: [normalized_state_early_discharge.decision_date desc, normalized_state_early_discharge.request_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 26
    col: 0
    width: 12
    height: 6

  - name: Normalized State Employment Period
    title: Normalized State Employment Period
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_employment_period.employer_address,
      normalized_state_employment_period.employer_name,
      normalized_state_employment_period.employment_period_id,
      normalized_state_employment_period.employment_status,
      normalized_state_employment_period.employment_status_raw_text,
      normalized_state_employment_period.end_date,
      normalized_state_employment_period.end_reason,
      normalized_state_employment_period.end_reason_raw_text,
      normalized_state_employment_period.external_id,
      normalized_state_employment_period.job_title,
      normalized_state_employment_period.last_verified_date,
      normalized_state_employment_period.start_date,
      normalized_state_employment_period.state_code]
    sorts: [normalized_state_employment_period.end_date desc, normalized_state_employment_period.last_verified_date desc, normalized_state_employment_period.start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 26
    col: 12
    width: 12
    height: 6

  - name: Normalized State Incarceration Incident
    title: Normalized State Incarceration Incident
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_incarceration_incident.external_id,
      normalized_state_incarceration_incident.facility,
      normalized_state_incarceration_incident.incarceration_incident_id,
      normalized_state_incarceration_incident.incident_date,
      normalized_state_incarceration_incident.incident_details,
      normalized_state_incarceration_incident.incident_metadata,
      normalized_state_incarceration_incident.incident_severity,
      normalized_state_incarceration_incident.incident_severity_raw_text,
      normalized_state_incarceration_incident.incident_type,
      normalized_state_incarceration_incident.incident_type_raw_text,
      normalized_state_incarceration_incident.location_within_facility,
      normalized_state_incarceration_incident.state_code]
    sorts: [normalized_state_incarceration_incident.incident_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 32
    col: 0
    width: 12
    height: 6

  - name: Normalized State Incarceration Incident Outcome
    title: Normalized State Incarceration Incident Outcome
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_incarceration_incident_outcome.date_effective_date,
      normalized_state_incarceration_incident_outcome.external_id,
      normalized_state_incarceration_incident_outcome.hearing_date,
      normalized_state_incarceration_incident_outcome.incarceration_incident_id,
      normalized_state_incarceration_incident_outcome.incarceration_incident_outcome_id,
      normalized_state_incarceration_incident_outcome.outcome_description,
      normalized_state_incarceration_incident_outcome.outcome_metadata,
      normalized_state_incarceration_incident_outcome.outcome_type,
      normalized_state_incarceration_incident_outcome.outcome_type_raw_text,
      normalized_state_incarceration_incident_outcome.projected_end_date,
      normalized_state_incarceration_incident_outcome.punishment_length_days,
      normalized_state_incarceration_incident_outcome.report_date,
      normalized_state_incarceration_incident_outcome.state_code]
    sorts: [normalized_state_incarceration_incident_outcome.date_effective_date desc, normalized_state_incarceration_incident_outcome.hearing_date desc, normalized_state_incarceration_incident_outcome.projected_end_date desc, normalized_state_incarceration_incident_outcome.report_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 32
    col: 12
    width: 12
    height: 6

  - name: Normalized State Incarceration Period
    title: Normalized State Incarceration Period
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_incarceration_period.admission_date,
      normalized_state_incarceration_period.admission_reason,
      normalized_state_incarceration_period.admission_reason_raw_text,
      normalized_state_incarceration_period.county_code,
      normalized_state_incarceration_period.custodial_authority,
      normalized_state_incarceration_period.custodial_authority_raw_text,
      normalized_state_incarceration_period.custody_level,
      normalized_state_incarceration_period.custody_level_raw_text,
      normalized_state_incarceration_period.external_id,
      normalized_state_incarceration_period.facility,
      normalized_state_incarceration_period.housing_unit,
      normalized_state_incarceration_period.housing_unit_category,
      normalized_state_incarceration_period.housing_unit_category_raw_text,
      normalized_state_incarceration_period.housing_unit_type,
      normalized_state_incarceration_period.housing_unit_type_raw_text,
      normalized_state_incarceration_period.incarceration_admission_violation_type,
      normalized_state_incarceration_period.incarceration_period_id,
      normalized_state_incarceration_period.incarceration_type,
      normalized_state_incarceration_period.incarceration_type_raw_text,
      normalized_state_incarceration_period.person_id,
      normalized_state_incarceration_period.purpose_for_incarceration_subtype,
      normalized_state_incarceration_period.release_date,
      normalized_state_incarceration_period.release_reason,
      normalized_state_incarceration_period.release_reason_raw_text,
      normalized_state_incarceration_period.sequence_num,
      normalized_state_incarceration_period.specialized_purpose_for_incarceration,
      normalized_state_incarceration_period.specialized_purpose_for_incarceration_raw_text,
      normalized_state_incarceration_period.state_code]
    sorts: [normalized_state_incarceration_period.admission_date desc, normalized_state_incarceration_period.release_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 38
    col: 0
    width: 12
    height: 6

  - name: Normalized State Incarceration Sentence
    title: Normalized State Incarceration Sentence
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_incarceration_sentence.completion_date,
      normalized_state_incarceration_sentence.conditions,
      normalized_state_incarceration_sentence.county_code,
      normalized_state_incarceration_sentence.date_imposed_date,
      normalized_state_incarceration_sentence.earned_time_days,
      normalized_state_incarceration_sentence.effective_date,
      normalized_state_incarceration_sentence.external_id,
      normalized_state_incarceration_sentence.good_time_days,
      normalized_state_incarceration_sentence.incarceration_sentence_id,
      normalized_state_incarceration_sentence.incarceration_type,
      normalized_state_incarceration_sentence.incarceration_type_raw_text,
      normalized_state_incarceration_sentence.initial_time_served_days,
      normalized_state_incarceration_sentence.is_capital_punishment,
      normalized_state_incarceration_sentence.is_life,
      normalized_state_incarceration_sentence.max_length_days,
      normalized_state_incarceration_sentence.min_length_days,
      normalized_state_incarceration_sentence.parole_eligibility_date,
      normalized_state_incarceration_sentence.parole_possible,
      normalized_state_incarceration_sentence.projected_max_release_date,
      normalized_state_incarceration_sentence.projected_min_release_date,
      normalized_state_incarceration_sentence.sentence_metadata,
      normalized_state_incarceration_sentence.state_code,
      normalized_state_incarceration_sentence.status,
      normalized_state_incarceration_sentence.status_raw_text]
    sorts: [normalized_state_incarceration_sentence.completion_date desc, normalized_state_incarceration_sentence.date_imposed_date desc, normalized_state_incarceration_sentence.effective_date desc, normalized_state_incarceration_sentence.parole_eligibility_date desc, normalized_state_incarceration_sentence.projected_max_release_date desc, normalized_state_incarceration_sentence.projected_min_release_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 38
    col: 12
    width: 12
    height: 6

  - name: Normalized State Person
    title: Normalized State Person
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_person.birthdate_date,
      normalized_state_person.current_address,
      normalized_state_person.current_email_address,
      normalized_state_person.current_phone_number,
      normalized_state_person.full_name,
      normalized_state_person.full_name_clean,
      normalized_state_person.gender,
      normalized_state_person.gender_raw_text,
      normalized_state_person.person_id,
      normalized_state_person.residency_status,
      normalized_state_person.residency_status_raw_text,
      normalized_state_person.state_code,
      normalized_state_person_ethnicity.count,
      normalized_state_person_race.count]
    sorts: [normalized_state_person.birthdate_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 44
    col: 0
    width: 12
    height: 6

  - name: Normalized State Person Address Period
    title: Normalized State Person Address Period
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_person_address_period.address_city,
      normalized_state_person_address_period.address_country,
      normalized_state_person_address_period.address_county,
      normalized_state_person_address_period.address_end_date,
      normalized_state_person_address_period.address_is_verified,
      normalized_state_person_address_period.address_line_1,
      normalized_state_person_address_period.address_line_2,
      normalized_state_person_address_period.address_metadata,
      normalized_state_person_address_period.address_start_date,
      normalized_state_person_address_period.address_state,
      normalized_state_person_address_period.address_type,
      normalized_state_person_address_period.address_type_raw_text,
      normalized_state_person_address_period.address_zip,
      normalized_state_person_address_period.full_address,
      normalized_state_person_address_period.person_address_period_id,
      normalized_state_person_address_period.state_code]
    sorts: [normalized_state_person_address_period.address_end_date desc, normalized_state_person_address_period.address_start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 44
    col: 12
    width: 12
    height: 6

  - name: Normalized State Person Alias
    title: Normalized State Person Alias
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_person_alias.alias_type,
      normalized_state_person_alias.alias_type_raw_text,
      normalized_state_person_alias.full_name,
      normalized_state_person_alias.person_alias_id,
      normalized_state_person_alias.state_code]
    sorts: []
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 50
    col: 0
    width: 12
    height: 6

  - name: Normalized State Person External Id
    title: Normalized State Person External Id
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_person_external_id.external_id,
      normalized_state_person_external_id.external_id_with_type,
      normalized_state_person_external_id.id_active_from_datetime_date,
      normalized_state_person_external_id.id_active_to_datetime_date,
      normalized_state_person_external_id.id_type,
      normalized_state_person_external_id.is_current_display_id_for_type,
      normalized_state_person_external_id.is_stable_id_for_type,
      normalized_state_person_external_id.person_external_id_id,
      normalized_state_person_external_id.state_code]
    sorts: [normalized_state_person_external_id.id_active_from_datetime_date desc, normalized_state_person_external_id.id_active_to_datetime_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 50
    col: 12
    width: 12
    height: 6

  - name: Normalized State Person Housing Status Period
    title: Normalized State Person Housing Status Period
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_person_housing_status_period.housing_status_end_date,
      normalized_state_person_housing_status_period.housing_status_start_date,
      normalized_state_person_housing_status_period.housing_status_type,
      normalized_state_person_housing_status_period.housing_status_type_raw_text,
      normalized_state_person_housing_status_period.person_housing_status_period_id,
      normalized_state_person_housing_status_period.state_code]
    sorts: [normalized_state_person_housing_status_period.housing_status_end_date desc, normalized_state_person_housing_status_period.housing_status_start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 56
    col: 0
    width: 12
    height: 6

  - name: Normalized State Person Staff Relationship Period
    title: Normalized State Person Staff Relationship Period
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_person_staff_relationship_period.associated_staff_external_id,
      normalized_state_person_staff_relationship_period.associated_staff_external_id_type,
      normalized_state_person_staff_relationship_period.associated_staff_id,
      normalized_state_person_staff_relationship_period.location_external_id,
      normalized_state_person_staff_relationship_period.person_staff_relationship_period_id,
      normalized_state_person_staff_relationship_period.relationship_end_date_exclusive_date,
      normalized_state_person_staff_relationship_period.relationship_priority,
      normalized_state_person_staff_relationship_period.relationship_start_date,
      normalized_state_person_staff_relationship_period.relationship_type,
      normalized_state_person_staff_relationship_period.relationship_type_raw_text,
      normalized_state_person_staff_relationship_period.state_code,
      normalized_state_person_staff_relationship_period.system_type,
      normalized_state_person_staff_relationship_period.system_type_raw_text]
    sorts: [normalized_state_person_staff_relationship_period.relationship_end_date_exclusive_date desc, normalized_state_person_staff_relationship_period.relationship_start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 56
    col: 12
    width: 12
    height: 6

  - name: Normalized State Program Assignment
    title: Normalized State Program Assignment
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_program_assignment.discharge_date,
      normalized_state_program_assignment.external_id,
      normalized_state_program_assignment.participation_status,
      normalized_state_program_assignment.participation_status_raw_text,
      normalized_state_program_assignment.program_assignment_id,
      normalized_state_program_assignment.program_id,
      normalized_state_program_assignment.program_location_id,
      normalized_state_program_assignment.referral_date,
      normalized_state_program_assignment.referral_metadata,
      normalized_state_program_assignment.referring_staff_external_id,
      normalized_state_program_assignment.referring_staff_external_id_type,
      normalized_state_program_assignment.referring_staff_id,
      normalized_state_program_assignment.sequence_num,
      normalized_state_program_assignment.start_date,
      normalized_state_program_assignment.state_code]
    sorts: [normalized_state_program_assignment.discharge_date desc, normalized_state_program_assignment.referral_date desc, normalized_state_program_assignment.start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 62
    col: 0
    width: 12
    height: 6

  - name: Normalized State Sentence
    title: Normalized State Sentence
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence.conditions,
      normalized_state_sentence.county_code,
      normalized_state_sentence.current_start_date,
      normalized_state_sentence.current_state_provided_start_date,
      normalized_state_sentence.external_id,
      normalized_state_sentence.imposed_date,
      normalized_state_sentence.initial_time_served_days,
      normalized_state_sentence.is_capital_punishment,
      normalized_state_sentence.is_life,
      normalized_state_sentence.parent_sentence_external_id_array,
      normalized_state_sentence.parole_possible,
      normalized_state_sentence.sentence_group_external_id,
      normalized_state_sentence.sentence_id,
      normalized_state_sentence.sentence_imposed_group_id,
      normalized_state_sentence.sentence_inferred_group_id,
      normalized_state_sentence.sentence_metadata,
      normalized_state_sentence.sentence_type,
      normalized_state_sentence.sentence_type_raw_text,
      normalized_state_sentence.sentencing_authority,
      normalized_state_sentence.sentencing_authority_raw_text,
      normalized_state_sentence.state_code]
    sorts: [normalized_state_sentence.current_start_date desc, normalized_state_sentence.current_state_provided_start_date desc, normalized_state_sentence.imposed_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 62
    col: 12
    width: 12
    height: 6

  - name: Normalized State Sentence Group
    title: Normalized State Sentence Group
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence_group.external_id,
      normalized_state_sentence_group.sentence_group_id,
      normalized_state_sentence_group.sentence_inferred_group_id,
      normalized_state_sentence_group.state_code]
    sorts: []
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 68
    col: 0
    width: 12
    height: 6

  - name: Normalized State Sentence Group Length
    title: Normalized State Sentence Group Length
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence_group_length.group_update_datetime_date,
      normalized_state_sentence_group_length.parole_eligibility_date_external_date,
      normalized_state_sentence_group_length.projected_full_term_release_date_max_external_date,
      normalized_state_sentence_group_length.projected_full_term_release_date_min_external_date,
      normalized_state_sentence_group_length.projected_parole_release_date_external_date,
      normalized_state_sentence_group_length.sentence_group_id,
      normalized_state_sentence_group_length.sentence_group_length_id,
      normalized_state_sentence_group_length.sequence_num,
      normalized_state_sentence_group_length.state_code]
    sorts: [normalized_state_sentence_group_length.group_update_datetime_date desc, normalized_state_sentence_group_length.parole_eligibility_date_external_date desc, normalized_state_sentence_group_length.projected_full_term_release_date_max_external_date desc, normalized_state_sentence_group_length.projected_full_term_release_date_min_external_date desc, normalized_state_sentence_group_length.projected_parole_release_date_external_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 68
    col: 12
    width: 12
    height: 6

  - name: Normalized State Sentence Imposed Group
    title: Normalized State Sentence Imposed Group
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence_imposed_group.external_id,
      normalized_state_sentence_imposed_group.imposed_date,
      normalized_state_sentence_imposed_group.most_severe_charge_v2_id,
      normalized_state_sentence_imposed_group.sentence_imposed_group_id,
      normalized_state_sentence_imposed_group.sentencing_authority,
      normalized_state_sentence_imposed_group.serving_start_date,
      normalized_state_sentence_imposed_group.state_code]
    sorts: [normalized_state_sentence_imposed_group.imposed_date desc, normalized_state_sentence_imposed_group.serving_start_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 74
    col: 0
    width: 12
    height: 6

  - name: Normalized State Sentence Inferred Group
    title: Normalized State Sentence Inferred Group
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence_inferred_group.external_id,
      normalized_state_sentence_inferred_group.sentence_inferred_group_id,
      normalized_state_sentence_inferred_group.state_code]
    sorts: []
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 74
    col: 12
    width: 12
    height: 6

  - name: Normalized State Sentence Length
    title: Normalized State Sentence Length
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence_length.earned_time_days,
      normalized_state_sentence_length.good_time_days,
      normalized_state_sentence_length.length_update_datetime_date,
      normalized_state_sentence_length.parole_eligibility_date_external_date,
      normalized_state_sentence_length.projected_completion_date_max_external_date,
      normalized_state_sentence_length.projected_completion_date_min_external_date,
      normalized_state_sentence_length.projected_parole_release_date_external_date,
      normalized_state_sentence_length.sentence_id,
      normalized_state_sentence_length.sentence_length_days_max,
      normalized_state_sentence_length.sentence_length_days_min,
      normalized_state_sentence_length.sentence_length_id,
      normalized_state_sentence_length.sequence_num,
      normalized_state_sentence_length.state_code]
    sorts: [normalized_state_sentence_length.length_update_datetime_date desc, normalized_state_sentence_length.parole_eligibility_date_external_date desc, normalized_state_sentence_length.projected_completion_date_max_external_date desc, normalized_state_sentence_length.projected_completion_date_min_external_date desc, normalized_state_sentence_length.projected_parole_release_date_external_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 80
    col: 0
    width: 12
    height: 6

  - name: Normalized State Sentence Status Snapshot
    title: Normalized State Sentence Status Snapshot
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_sentence_status_snapshot.sentence_id,
      normalized_state_sentence_status_snapshot.sentence_status_snapshot_id,
      normalized_state_sentence_status_snapshot.sequence_num,
      normalized_state_sentence_status_snapshot.state_code,
      normalized_state_sentence_status_snapshot.status,
      normalized_state_sentence_status_snapshot.status_end_datetime_date,
      normalized_state_sentence_status_snapshot.status_raw_text,
      normalized_state_sentence_status_snapshot.status_update_datetime_date]
    sorts: [normalized_state_sentence_status_snapshot.status_end_datetime_date desc, normalized_state_sentence_status_snapshot.status_update_datetime_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 80
    col: 12
    width: 12
    height: 6

  - name: Normalized State Supervision Contact
    title: Normalized State Supervision Contact
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_supervision_contact.contact_date,
      normalized_state_supervision_contact.contact_method,
      normalized_state_supervision_contact.contact_method_raw_text,
      normalized_state_supervision_contact.contact_reason,
      normalized_state_supervision_contact.contact_reason_raw_text,
      normalized_state_supervision_contact.contact_type,
      normalized_state_supervision_contact.contact_type_raw_text,
      normalized_state_supervision_contact.contacting_staff_external_id,
      normalized_state_supervision_contact.contacting_staff_external_id_type,
      normalized_state_supervision_contact.contacting_staff_id,
      normalized_state_supervision_contact.external_id,
      normalized_state_supervision_contact.location,
      normalized_state_supervision_contact.location_raw_text,
      normalized_state_supervision_contact.resulted_in_arrest,
      normalized_state_supervision_contact.scheduled_contact_date,
      normalized_state_supervision_contact.state_code,
      normalized_state_supervision_contact.status,
      normalized_state_supervision_contact.status_raw_text,
      normalized_state_supervision_contact.supervision_contact_id,
      normalized_state_supervision_contact.supervision_contact_metadata,
      normalized_state_supervision_contact.verified_employment]
    sorts: [normalized_state_supervision_contact.contact_date desc, normalized_state_supervision_contact.scheduled_contact_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 86
    col: 0
    width: 12
    height: 6

  - name: Normalized State Supervision Period
    title: Normalized State Supervision Period
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_supervision_case_type_entry.count,
      normalized_state_supervision_period.admission_reason,
      normalized_state_supervision_period.admission_reason_raw_text,
      normalized_state_supervision_period.conditions,
      normalized_state_supervision_period.county_code,
      normalized_state_supervision_period.custodial_authority,
      normalized_state_supervision_period.custodial_authority_raw_text,
      normalized_state_supervision_period.external_id,
      normalized_state_supervision_period.person_id,
      normalized_state_supervision_period.sequence_num,
      normalized_state_supervision_period.start_date,
      normalized_state_supervision_period.state_code,
      normalized_state_supervision_period.supervising_officer_staff_external_id,
      normalized_state_supervision_period.supervising_officer_staff_external_id_type,
      normalized_state_supervision_period.supervising_officer_staff_id,
      normalized_state_supervision_period.supervision_level,
      normalized_state_supervision_period.supervision_level_raw_text,
      normalized_state_supervision_period.supervision_period_id,
      normalized_state_supervision_period.supervision_period_metadata,
      normalized_state_supervision_period.supervision_site,
      normalized_state_supervision_period.supervision_type,
      normalized_state_supervision_period.supervision_type_raw_text,
      normalized_state_supervision_period.termination_date,
      normalized_state_supervision_period.termination_reason,
      normalized_state_supervision_period.termination_reason_raw_text]
    sorts: [normalized_state_supervision_period.start_date desc, normalized_state_supervision_period.termination_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 86
    col: 12
    width: 12
    height: 6

  - name: Normalized State Supervision Sentence
    title: Normalized State Supervision Sentence
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_supervision_sentence.completion_date,
      normalized_state_supervision_sentence.conditions,
      normalized_state_supervision_sentence.county_code,
      normalized_state_supervision_sentence.date_imposed_date,
      normalized_state_supervision_sentence.effective_date,
      normalized_state_supervision_sentence.external_id,
      normalized_state_supervision_sentence.is_life,
      normalized_state_supervision_sentence.max_length_days,
      normalized_state_supervision_sentence.min_length_days,
      normalized_state_supervision_sentence.projected_completion_date,
      normalized_state_supervision_sentence.sentence_metadata,
      normalized_state_supervision_sentence.state_code,
      normalized_state_supervision_sentence.status,
      normalized_state_supervision_sentence.status_raw_text,
      normalized_state_supervision_sentence.supervision_sentence_id,
      normalized_state_supervision_sentence.supervision_type,
      normalized_state_supervision_sentence.supervision_type_raw_text]
    sorts: [normalized_state_supervision_sentence.completion_date desc, normalized_state_supervision_sentence.date_imposed_date desc, normalized_state_supervision_sentence.effective_date desc, normalized_state_supervision_sentence.projected_completion_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 92
    col: 0
    width: 12
    height: 6

  - name: Normalized State Supervision Violation
    title: Normalized State Supervision Violation
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_supervision_violated_condition_entry.count,
      normalized_state_supervision_violation.external_id,
      normalized_state_supervision_violation.is_sex_offense,
      normalized_state_supervision_violation.is_violent,
      normalized_state_supervision_violation.state_code,
      normalized_state_supervision_violation.supervision_violation_id,
      normalized_state_supervision_violation.violation_date,
      normalized_state_supervision_violation.violation_metadata,
      normalized_state_supervision_violation.violation_severity,
      normalized_state_supervision_violation.violation_severity_raw_text,
      normalized_state_supervision_violation_type_entry.count]
    sorts: [normalized_state_supervision_violation.violation_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 92
    col: 12
    width: 12
    height: 6

  - name: Normalized State Supervision Violation Response
    title: Normalized State Supervision Violation Response
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_supervision_violation_response.deciding_body_type,
      normalized_state_supervision_violation_response.deciding_body_type_raw_text,
      normalized_state_supervision_violation_response.deciding_staff_external_id,
      normalized_state_supervision_violation_response.deciding_staff_external_id_type,
      normalized_state_supervision_violation_response.deciding_staff_id,
      normalized_state_supervision_violation_response.external_id,
      normalized_state_supervision_violation_response.is_draft,
      normalized_state_supervision_violation_response.response_date,
      normalized_state_supervision_violation_response.response_subtype,
      normalized_state_supervision_violation_response.response_type,
      normalized_state_supervision_violation_response.response_type_raw_text,
      normalized_state_supervision_violation_response.sequence_num,
      normalized_state_supervision_violation_response.state_code,
      normalized_state_supervision_violation_response.supervision_violation_id,
      normalized_state_supervision_violation_response.supervision_violation_response_id,
      normalized_state_supervision_violation_response.violation_response_metadata,
      normalized_state_supervision_violation_response.violation_response_severity,
      normalized_state_supervision_violation_response.violation_response_severity_raw_text,
      normalized_state_supervision_violation_response_decision_entry.count]
    sorts: [normalized_state_supervision_violation_response.response_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 98
    col: 0
    width: 12
    height: 6

  - name: Normalized State Task Deadline
    title: Normalized State Task Deadline
    explore: normalized_state_person
    model: recidiviz-staging
    type: looker_grid
    fields: [normalized_state_task_deadline.due_date,
      normalized_state_task_deadline.eligible_date,
      normalized_state_task_deadline.sequence_num,
      normalized_state_task_deadline.state_code,
      normalized_state_task_deadline.task_deadline_id,
      normalized_state_task_deadline.task_metadata,
      normalized_state_task_deadline.task_subtype,
      normalized_state_task_deadline.task_type,
      normalized_state_task_deadline.task_type_raw_text,
      normalized_state_task_deadline.update_datetime_date]
    sorts: [normalized_state_task_deadline.due_date desc, normalized_state_task_deadline.eligible_date desc, normalized_state_task_deadline.update_datetime_date desc]
    listen: 
      Person Id: normalized_state_person.person_id
      State Code: normalized_state_person.state_code
      External Id: normalized_state_person_external_id.external_id
      Id Type: normalized_state_person_external_id.id_type
    row: 98
    col: 12
    width: 12
    height: 6

