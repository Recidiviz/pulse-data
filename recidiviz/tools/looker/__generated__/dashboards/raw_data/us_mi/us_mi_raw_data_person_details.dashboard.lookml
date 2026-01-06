# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_mi_raw_data_person_details
  title: Michigan Raw Data Person Details
  description: For examining individuals in US_MI's raw data tables
  layout: newspaper
  load_configuration: wait

  filters:
  - name: View Type
    title: View Type
    type: field_filter
    default_value: raw^_data^_up^_to^_date^_views
    allow_multiple_values: false
    required: true
    ui_config: 
      type: dropdown_menu
      display: inline
    model: "@{model_name}"
    explore: us_mi_raw_data
    field: us_mi_ADH_OFFENDER.view_type

  - name: US_MI_DOC_ID
    title: US_MI_DOC_ID
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mi_raw_data
    field: us_mi_ADH_OFFENDER.offender_id

  - name: US_MI_DOC
    title: US_MI_DOC
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mi_raw_data
    field: us_mi_ADH_OFFENDER.offender_number

  - name: US_MI_DOC_BOOK
    title: US_MI_DOC_BOOK
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mi_raw_data
    field: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id

  elements:
  - name: ADH_OFFENDER
    title: ADH_OFFENDER
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER.primary_key,
      us_mi_ADH_OFFENDER.offender_id,
      us_mi_ADH_OFFENDER.offender_number_source_id,
      us_mi_ADH_OFFENDER.offender_number,
      us_mi_ADH_OFFENDER.last_update_user,
      us_mi_ADH_OFFENDER.last_update_date,
      us_mi_ADH_OFFENDER.last_update_node,
      us_mi_ADH_OFFENDER.file_id,
      us_mi_ADH_OFFENDER.is_deleted]
    sorts: [us_mi_ADH_OFFENDER.offender_id, us_mi_ADH_OFFENDER.offender_number]
    note_display: hover
    note_text: "This is the main table that contains all of the identifiers used for all other tables for all persons currently involved in MIDOC (parole, probation, incarceration)."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 0
    col: 0
    width: 24
    height: 6

  - name: ADH_FACILITY_COUNT_SHEET
    title: ADH_FACILITY_COUNT_SHEET
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_FACILITY_COUNT_SHEET.primary_key,
      us_mi_ADH_FACILITY_COUNT_SHEET.facility_count_sheet_id,
      us_mi_ADH_FACILITY_COUNT_SHEET.facility_count_unit_id,
      us_mi_ADH_FACILITY_COUNT_SHEET.cell,
      us_mi_ADH_FACILITY_COUNT_SHEET.bunk,
      us_mi_ADH_FACILITY_COUNT_SHEET.offender_number,
      us_mi_ADH_FACILITY_COUNT_SHEET.offender_last_name,
      us_mi_ADH_FACILITY_COUNT_SHEET.out_flag,
      us_mi_ADH_FACILITY_COUNT_SHEET.empty_flag,
      us_mi_ADH_FACILITY_COUNT_SHEET.sequence_number,
      us_mi_ADH_FACILITY_COUNT_SHEET.last_update_user,
      us_mi_ADH_FACILITY_COUNT_SHEET.last_update_date__raw,
      us_mi_ADH_FACILITY_COUNT_SHEET.last_update_node,
      us_mi_ADH_FACILITY_COUNT_SHEET.unit_lock_id,
      us_mi_ADH_FACILITY_COUNT_SHEET.in_flg,
      us_mi_ADH_FACILITY_COUNT_SHEET.employee_id,
      us_mi_ADH_FACILITY_COUNT_SHEET.submit_date,
      us_mi_ADH_FACILITY_COUNT_SHEET.temperature,
      us_mi_ADH_FACILITY_COUNT_SHEET.perm_temp_flg,
      us_mi_ADH_FACILITY_COUNT_SHEET.visitor_flag,
      us_mi_ADH_FACILITY_COUNT_SHEET.file_id,
      us_mi_ADH_FACILITY_COUNT_SHEET.is_deleted]
    sorts: [us_mi_ADH_FACILITY_COUNT_SHEET.last_update_date__raw]
    note_display: hover
    note_text: "Table containing the most recent facility count sheet"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 6
    col: 0
    width: 24
    height: 6

  - name: ADH_FACILITY_COUNT_SHEET_HIST
    title: ADH_FACILITY_COUNT_SHEET_HIST
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_FACILITY_COUNT_SHEET_HIST.primary_key,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.facility_count_sheet_id,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.facility_count_unit_id,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.cell,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.bunk,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.offender_number,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.offender_last_name,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.out_flag,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.empty_flag,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.sequence_number,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.last_update_user,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.last_update_date__raw,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.last_update_node,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.unit_lock_id,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.in_flg,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.employee_id,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.submit_date,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.temperature,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.perm_temp_flg,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.visitor_flag,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.file_id,
      us_mi_ADH_FACILITY_COUNT_SHEET_HIST.is_deleted]
    sorts: [us_mi_ADH_FACILITY_COUNT_SHEET_HIST.last_update_date__raw]
    note_display: hover
    note_text: "Table containing all historical facility counts in MIDOC."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 12
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_BOOKING
    title: ADH_OFFENDER_BOOKING
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_BOOKING.primary_key,
      us_mi_ADH_OFFENDER_BOOKING.offender_booking_id,
      us_mi_ADH_OFFENDER_BOOKING.offender_booking_number,
      us_mi_ADH_OFFENDER_BOOKING.offender_id,
      us_mi_ADH_OFFENDER_BOOKING.offender_name_seq_no,
      us_mi_ADH_OFFENDER_BOOKING.location_id,
      us_mi_ADH_OFFENDER_BOOKING.begin_date__raw,
      us_mi_ADH_OFFENDER_BOOKING.end_date__raw,
      us_mi_ADH_OFFENDER_BOOKING.previous_offender_id,
      us_mi_ADH_OFFENDER_BOOKING.next_offender_id,
      us_mi_ADH_OFFENDER_BOOKING.active_flag,
      us_mi_ADH_OFFENDER_BOOKING.disclosure_flag,
      us_mi_ADH_OFFENDER_BOOKING.in_out_flag,
      us_mi_ADH_OFFENDER_BOOKING.last_update_user,
      us_mi_ADH_OFFENDER_BOOKING.last_update_date,
      us_mi_ADH_OFFENDER_BOOKING.last_update_node,
      us_mi_ADH_OFFENDER_BOOKING.file_id,
      us_mi_ADH_OFFENDER_BOOKING.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_BOOKING.begin_date__raw]
    note_display: hover
    note_text: "This table stores information about jurisdictions - periods of time when a justice-involved individual spends time within the MDOC."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 18
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_DESIGNATION
    title: ADH_OFFENDER_DESIGNATION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_DESIGNATION.primary_key,
      us_mi_ADH_OFFENDER_DESIGNATION.offender_designation_id,
      us_mi_ADH_OFFENDER_DESIGNATION.offender_id,
      us_mi_ADH_OFFENDER_DESIGNATION.offender_designation_code_id,
      us_mi_ADH_OFFENDER_DESIGNATION.start_date__raw,
      us_mi_ADH_OFFENDER_DESIGNATION.end_date__raw,
      us_mi_ADH_OFFENDER_DESIGNATION.last_update_user,
      us_mi_ADH_OFFENDER_DESIGNATION.last_update_date__raw,
      us_mi_ADH_OFFENDER_DESIGNATION.last_update_node__raw,
      us_mi_ADH_OFFENDER_DESIGNATION.file_id,
      us_mi_ADH_OFFENDER_DESIGNATION.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_DESIGNATION.start_date__raw]
    note_display: hover
    note_text: "This table contains data for each individual's designation (such as administrative segregation) in the MDOC system"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 24
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_DETAINER
    title: ADH_OFFENDER_DETAINER
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_DETAINER.primary_key,
      us_mi_ADH_OFFENDER_DETAINER.detainer_id,
      us_mi_ADH_OFFENDER_DETAINER.offender_booking_id,
      us_mi_ADH_OFFENDER_DETAINER.detainer_sequence_number,
      us_mi_ADH_OFFENDER_DETAINER.detainer_received_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.detainer_type_id,
      us_mi_ADH_OFFENDER_DETAINER.offender_id,
      us_mi_ADH_OFFENDER_DETAINER.originating_agency,
      us_mi_ADH_OFFENDER_DETAINER.notify_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.revoked_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.ori_number,
      us_mi_ADH_OFFENDER_DETAINER.contact_person,
      us_mi_ADH_OFFENDER_DETAINER.address1,
      us_mi_ADH_OFFENDER_DETAINER.address2,
      us_mi_ADH_OFFENDER_DETAINER.address3,
      us_mi_ADH_OFFENDER_DETAINER.city,
      us_mi_ADH_OFFENDER_DETAINER.state_id,
      us_mi_ADH_OFFENDER_DETAINER.postal_code,
      us_mi_ADH_OFFENDER_DETAINER.country_id,
      us_mi_ADH_OFFENDER_DETAINER.contact_phone,
      us_mi_ADH_OFFENDER_DETAINER.reason,
      us_mi_ADH_OFFENDER_DETAINER.complaint_number,
      us_mi_ADH_OFFENDER_DETAINER.written_verification_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.request_for_clearance_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.clearance_method_lein,
      us_mi_ADH_OFFENDER_DETAINER.clearance_method_letter,
      us_mi_ADH_OFFENDER_DETAINER.clearance_method_other,
      us_mi_ADH_OFFENDER_DETAINER.inactive_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.last_update_user,
      us_mi_ADH_OFFENDER_DETAINER.last_update_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.last_update_node,
      us_mi_ADH_OFFENDER_DETAINER.detainer_source_id,
      us_mi_ADH_OFFENDER_DETAINER.dist_court_no,
      us_mi_ADH_OFFENDER_DETAINER.circuit_court_no,
      us_mi_ADH_OFFENDER_DETAINER.contact_phone_ext,
      us_mi_ADH_OFFENDER_DETAINER.contact_email,
      us_mi_ADH_OFFENDER_DETAINER.extradition_req_flg,
      us_mi_ADH_OFFENDER_DETAINER.deportation_eligible_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.deportation_eligible_flg,
      us_mi_ADH_OFFENDER_DETAINER.parole_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.deportation_final_order_flg,
      us_mi_ADH_OFFENDER_DETAINER.assurance_from_ice_flg,
      us_mi_ADH_OFFENDER_DETAINER.notes,
      us_mi_ADH_OFFENDER_DETAINER.active_flag,
      us_mi_ADH_OFFENDER_DETAINER.expiration_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.county,
      us_mi_ADH_OFFENDER_DETAINER.followup_req,
      us_mi_ADH_OFFENDER_DETAINER.followup_reason_id,
      us_mi_ADH_OFFENDER_DETAINER.followup_date__raw,
      us_mi_ADH_OFFENDER_DETAINER.file_id,
      us_mi_ADH_OFFENDER_DETAINER.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_DETAINER.detainer_received_date__raw]
    note_display: hover
    note_text: "This table contains information on detainers issued for people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 30
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_ERD
    title: ADH_OFFENDER_ERD
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_ERD.primary_key,
      us_mi_ADH_OFFENDER_ERD.offender_erd_id,
      us_mi_ADH_OFFENDER_ERD.offender_id,
      us_mi_ADH_OFFENDER_ERD.offender_erd_date__raw,
      us_mi_ADH_OFFENDER_ERD.last_update_user,
      us_mi_ADH_OFFENDER_ERD.last_update_date__raw,
      us_mi_ADH_OFFENDER_ERD.last_update_node,
      us_mi_ADH_OFFENDER_ERD.file_id,
      us_mi_ADH_OFFENDER_ERD.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_ERD.offender_erd_date__raw]
    note_display: hover
    note_text: "This table contains ERD (early release date) information for people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 36
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_LOCK
    title: ADH_OFFENDER_LOCK
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_LOCK.primary_key,
      us_mi_ADH_OFFENDER_LOCK.offender_lock_id,
      us_mi_ADH_OFFENDER_LOCK.offender_id,
      us_mi_ADH_OFFENDER_LOCK.unit_lock_id,
      us_mi_ADH_OFFENDER_LOCK.location_id,
      us_mi_ADH_OFFENDER_LOCK.date_in__raw,
      us_mi_ADH_OFFENDER_LOCK.date_out__raw,
      us_mi_ADH_OFFENDER_LOCK.permanent_temporary_flag,
      us_mi_ADH_OFFENDER_LOCK.last_update_user,
      us_mi_ADH_OFFENDER_LOCK.last_update_date__raw,
      us_mi_ADH_OFFENDER_LOCK.last_update_node,
      us_mi_ADH_OFFENDER_LOCK.file_id,
      us_mi_ADH_OFFENDER_LOCK.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_LOCK.date_in__raw]
    note_display: hover
    note_text: "For individuals who are incarcerated at MDOC facilities, this table contains information regarding their unit in the correctional facility."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 42
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_NAME
    title: ADH_OFFENDER_NAME
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_NAME.primary_key,
      us_mi_ADH_OFFENDER_NAME.offender_id,
      us_mi_ADH_OFFENDER_NAME.sequence_number,
      us_mi_ADH_OFFENDER_NAME.offender_name_type_id,
      us_mi_ADH_OFFENDER_NAME.last_name,
      us_mi_ADH_OFFENDER_NAME.first_name,
      us_mi_ADH_OFFENDER_NAME.middle_name,
      us_mi_ADH_OFFENDER_NAME.name_suffix,
      us_mi_ADH_OFFENDER_NAME.birth_date__raw,
      us_mi_ADH_OFFENDER_NAME.birth_city,
      us_mi_ADH_OFFENDER_NAME.birth_state_id,
      us_mi_ADH_OFFENDER_NAME.birth_country_id,
      us_mi_ADH_OFFENDER_NAME.sex_id,
      us_mi_ADH_OFFENDER_NAME.create_date,
      us_mi_ADH_OFFENDER_NAME.last_name_soundex,
      us_mi_ADH_OFFENDER_NAME.last_name_lookup,
      us_mi_ADH_OFFENDER_NAME.last_update_user,
      us_mi_ADH_OFFENDER_NAME.last_update_date,
      us_mi_ADH_OFFENDER_NAME.last_update_node,
      us_mi_ADH_OFFENDER_NAME.file_id,
      us_mi_ADH_OFFENDER_NAME.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_NAME.birth_date__raw]
    note_display: hover
    note_text: "This table contains the entries of basic information of each person involved under  MDOC's jurisdiction, with the sequence number being associated with the latest jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 48
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_PROFILE_SUMMARY_WRK
    title: ADH_OFFENDER_PROFILE_SUMMARY_WRK
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.primary_key,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.offender_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.offender_number,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.offender_booking_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.last_name,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.first_name,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.middle_name,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.name_suffix,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.birth_date__raw,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.eye_color_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.eye_color,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.hair_color_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.hair_color,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.race_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.race,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.gender_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.gender,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.height,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.weight,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.city,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.county_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.county,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.profile_date,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.ofr_sequence_number,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.active_flag,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.disclosure_flag,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.hyta_flag,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.location_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.location_name,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.profile_booking_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.build_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.build,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.skin_complexion_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.skin_complexion,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.marital_status_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.marital_status,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.first_arrest_age,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.csc_convictions_count,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.prior_felony_conviction_count,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.age,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.prior_misdem_convict_count,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.offender_name_seq_no,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.last_update_date,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.file_id,
      us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.birth_date__raw]
    note_display: hover
    note_text: "This table holds profile information about a person involved in MDOC that is linked to a given booking."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 54
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_SENTENCE
    title: ADH_OFFENDER_SENTENCE
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_SENTENCE.primary_key,
      us_mi_ADH_OFFENDER_SENTENCE.offender_sentence_id,
      us_mi_ADH_OFFENDER_SENTENCE.offender_charge_id,
      us_mi_ADH_OFFENDER_SENTENCE.legal_order_id,
      us_mi_ADH_OFFENDER_SENTENCE.offender_id,
      us_mi_ADH_OFFENDER_SENTENCE.offender_booking_id,
      us_mi_ADH_OFFENDER_SENTENCE.sentence_record_no,
      us_mi_ADH_OFFENDER_SENTENCE.monetary_only,
      us_mi_ADH_OFFENDER_SENTENCE.sentence_type_id,
      us_mi_ADH_OFFENDER_SENTENCE.sentence_subtype_id,
      us_mi_ADH_OFFENDER_SENTENCE.sentence_calc_type_id,
      us_mi_ADH_OFFENDER_SENTENCE.sentence_status_id,
      us_mi_ADH_OFFENDER_SENTENCE.sentencing_status_id,
      us_mi_ADH_OFFENDER_SENTENCE.judge_id,
      us_mi_ADH_OFFENDER_SENTENCE.counts,
      us_mi_ADH_OFFENDER_SENTENCE.review_flag_id,
      us_mi_ADH_OFFENDER_SENTENCE.effective_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.sentenced_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.corrected_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.probation_jail_months,
      us_mi_ADH_OFFENDER_SENTENCE.probation_jail_days,
      us_mi_ADH_OFFENDER_SENTENCE.min_length_years,
      us_mi_ADH_OFFENDER_SENTENCE.min_length_months,
      us_mi_ADH_OFFENDER_SENTENCE.min_length_days,
      us_mi_ADH_OFFENDER_SENTENCE.min_abs_flag,
      us_mi_ADH_OFFENDER_SENTENCE.min_life_flag,
      us_mi_ADH_OFFENDER_SENTENCE.min_credits_begin_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.min_credits_end_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.pmi_sgt_min_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.ami_rgt_min_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.calendar_min_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.max_length_years,
      us_mi_ADH_OFFENDER_SENTENCE.max_length_months,
      us_mi_ADH_OFFENDER_SENTENCE.max_length_days,
      us_mi_ADH_OFFENDER_SENTENCE.max_abs_flag,
      us_mi_ADH_OFFENDER_SENTENCE.max_life_flag,
      us_mi_ADH_OFFENDER_SENTENCE.max_credits_begin_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.max_credits_end_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.pmx_sgt_max_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.amx_rgt_max_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.consecutive_flag,
      us_mi_ADH_OFFENDER_SENTENCE.controlling_sent_code_id,
      us_mi_ADH_OFFENDER_SENTENCE.controlling_min_sentence_flag,
      us_mi_ADH_OFFENDER_SENTENCE.controlling_max_sentence_flag,
      us_mi_ADH_OFFENDER_SENTENCE.prior_minimum_sentence_id,
      us_mi_ADH_OFFENDER_SENTENCE.prior_maximum_sentence_id,
      us_mi_ADH_OFFENDER_SENTENCE.next_minimum_sentence_id,
      us_mi_ADH_OFFENDER_SENTENCE.next_maximum_sentence_id,
      us_mi_ADH_OFFENDER_SENTENCE.parole_to_min_max_id,
      us_mi_ADH_OFFENDER_SENTENCE.expiration_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.closing_reason_code,
      us_mi_ADH_OFFENDER_SENTENCE.closing_authority_id,
      us_mi_ADH_OFFENDER_SENTENCE.closing_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.closing_notes,
      us_mi_ADH_OFFENDER_SENTENCE.notes,
      us_mi_ADH_OFFENDER_SENTENCE.last_update_user,
      us_mi_ADH_OFFENDER_SENTENCE.last_update_date__raw,
      us_mi_ADH_OFFENDER_SENTENCE.last_update_node,
      us_mi_ADH_OFFENDER_SENTENCE.lifetime_gps_flag,
      us_mi_ADH_OFFENDER_SENTENCE.file_id,
      us_mi_ADH_OFFENDER_SENTENCE.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_SENTENCE.effective_date__raw]
    note_display: hover
    note_text: "This table contains sentence information for people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 60
    col: 0
    width: 24
    height: 6

  - name: ADH_PERSON
    title: ADH_PERSON
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_PERSON.primary_key,
      us_mi_ADH_PERSON.person_id,
      us_mi_ADH_PERSON.last_name,
      us_mi_ADH_PERSON.first_name,
      us_mi_ADH_PERSON.middle_name,
      us_mi_ADH_PERSON.name_suffix,
      us_mi_ADH_PERSON.birth_date__raw,
      us_mi_ADH_PERSON.month_year_flag,
      us_mi_ADH_PERSON.year_flag,
      us_mi_ADH_PERSON.deceased_flag,
      us_mi_ADH_PERSON.address1,
      us_mi_ADH_PERSON.address2,
      us_mi_ADH_PERSON.address3,
      us_mi_ADH_PERSON.city,
      us_mi_ADH_PERSON.state_id,
      us_mi_ADH_PERSON.postal_code,
      us_mi_ADH_PERSON.country_id,
      us_mi_ADH_PERSON.business_phone_country_code,
      us_mi_ADH_PERSON.business_phone_area_code,
      us_mi_ADH_PERSON.business_phone_number,
      us_mi_ADH_PERSON.business_phone_extension,
      us_mi_ADH_PERSON.business_fax_number,
      us_mi_ADH_PERSON.home_phone_country_code,
      us_mi_ADH_PERSON.home_phone_area_code,
      us_mi_ADH_PERSON.home_phone_number,
      us_mi_ADH_PERSON.home_phone_extension,
      us_mi_ADH_PERSON.notes,
      us_mi_ADH_PERSON.occupation,
      us_mi_ADH_PERSON.offender_id,
      us_mi_ADH_PERSON.last_name_lookup,
      us_mi_ADH_PERSON.last_update_user,
      us_mi_ADH_PERSON.last_update_date__raw,
      us_mi_ADH_PERSON.last_update_node,
      us_mi_ADH_PERSON.email,
      us_mi_ADH_PERSON.file_id,
      us_mi_ADH_PERSON.is_deleted]
    sorts: [us_mi_ADH_PERSON.birth_date__raw]
    note_display: hover
    note_text: "This table includes contact information for justice-involved individuals in the MDOC system."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 66
    col: 0
    width: 24
    height: 6

  - name: ADH_PERSONAL_PROTECTION_ORDER
    title: ADH_PERSONAL_PROTECTION_ORDER
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_PERSONAL_PROTECTION_ORDER.primary_key,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.personal_protection_order_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.offender_booking_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.offender_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.effective_date__raw,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.expiration_date__raw,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.issuing_location_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.protected_person_last_name,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.protected_person_first_name,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.protected_person_middle_name,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.protectted_person_suffix,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.court_file_no,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.notes,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.last_update_user,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.last_update_date__raw,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.last_update_node,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.file_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER.is_deleted]
    sorts: [us_mi_ADH_PERSONAL_PROTECTION_ORDER.effective_date__raw]
    note_display: hover
    note_text: "This table contains information on personal protection orders issued against people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 72
    col: 0
    width: 24
    height: 6

  - name: ADH_SHOFFENDER
    title: ADH_SHOFFENDER
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_SHOFFENDER.primary_key,
      us_mi_ADH_SHOFFENDER.FkShPerson,
      us_mi_ADH_SHOFFENDER.FkShAgency,
      us_mi_ADH_SHOFFENDER.OffenderNumber,
      us_mi_ADH_SHOFFENDER.SecondOffenderNumber,
      us_mi_ADH_SHOFFENDER.ThirdOffenderNumber,
      us_mi_ADH_SHOFFENDER.ForthOffenderNumber,
      us_mi_ADH_SHOFFENDER.MugShotImagesCount,
      us_mi_ADH_SHOFFENDER.FkShAgencyCreatedBy,
      us_mi_ADH_SHOFFENDER.IsClosed,
      us_mi_ADH_SHOFFENDER.IsRestricted,
      us_mi_ADH_SHOFFENDER.file_id,
      us_mi_ADH_SHOFFENDER.is_deleted]
    sorts: [us_mi_ADH_SHOFFENDER.FkShPerson]
    note_display: hover
    note_text: "This table contains information on people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 78
    col: 0
    width: 24
    height: 6

  - name: COMS_Assaultive_Risk_Assessments
    title: COMS_Assaultive_Risk_Assessments
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Assaultive_Risk_Assessments.primary_key,
      us_mi_COMS_Assaultive_Risk_Assessments.Offender_Number,
      us_mi_COMS_Assaultive_Risk_Assessments.Assessed_Date__raw,
      us_mi_COMS_Assaultive_Risk_Assessments.Assessed_By_Staff,
      us_mi_COMS_Assaultive_Risk_Assessments.Docket_Number,
      us_mi_COMS_Assaultive_Risk_Assessments.Assessment_Decision_Type,
      us_mi_COMS_Assaultive_Risk_Assessments.Assessment_Decision_Category,
      us_mi_COMS_Assaultive_Risk_Assessments.Calculated_Score,
      us_mi_COMS_Assaultive_Risk_Assessments.Calculated_Result,
      us_mi_COMS_Assaultive_Risk_Assessments.Override_Staff,
      us_mi_COMS_Assaultive_Risk_Assessments.Override_Result,
      us_mi_COMS_Assaultive_Risk_Assessments.Assessment_Result,
      us_mi_COMS_Assaultive_Risk_Assessments.Next_Review_Date__raw,
      us_mi_COMS_Assaultive_Risk_Assessments.Entered_Date__raw,
      us_mi_COMS_Assaultive_Risk_Assessments.file_id,
      us_mi_COMS_Assaultive_Risk_Assessments.is_deleted]
    sorts: [us_mi_COMS_Assaultive_Risk_Assessments.Assessed_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the assaultive risk assessment results."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 84
    col: 0
    width: 24
    height: 6

  - name: COMS_Case_Managers
    title: COMS_Case_Managers
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Case_Managers.primary_key,
      us_mi_COMS_Case_Managers.Case_Manager_Id,
      us_mi_COMS_Case_Managers.Supervision_Status_Id,
      us_mi_COMS_Case_Managers.Offender_Number,
      us_mi_COMS_Case_Managers.Case_Manager_Omnni_Employee_Id,
      us_mi_COMS_Case_Managers.Start_Date__raw,
      us_mi_COMS_Case_Managers.End_Date__raw,
      us_mi_COMS_Case_Managers.Entered_Date__raw,
      us_mi_COMS_Case_Managers.file_id,
      us_mi_COMS_Case_Managers.is_deleted]
    sorts: [us_mi_COMS_Case_Managers.Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the assignment of supervision agents to JII on supervision.  This table is active starting 8/14/2023 and should only hold supervision assignment information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 90
    col: 0
    width: 24
    height: 6

  - name: COMS_Case_Notes
    title: COMS_Case_Notes
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Case_Notes.primary_key,
      us_mi_COMS_Case_Notes.Case_Note_Id,
      us_mi_COMS_Case_Notes.Offender_Number,
      us_mi_COMS_Case_Notes.Note_Date__raw,
      us_mi_COMS_Case_Notes.Note,
      us_mi_COMS_Case_Notes.Entered_Date__raw,
      us_mi_COMS_Case_Notes.file_id,
      us_mi_COMS_Case_Notes.is_deleted]
    sorts: [us_mi_COMS_Case_Notes.Note_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system for case notes for the following activities:   - Court Hearing   - In Person Contact   - Probation Early Discharge Review   - Security Classification Committee activities Due to the size of this table, this file is provided to us as two week diffs"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 96
    col: 0
    width: 24
    height: 6

  - name: COMS_Employment
    title: COMS_Employment
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Employment.primary_key,
      us_mi_COMS_Employment.Employment_Id,
      us_mi_COMS_Employment.Offender_Number,
      us_mi_COMS_Employment.Job_Status,
      us_mi_COMS_Employment.Job_Type,
      us_mi_COMS_Employment.Job_Title,
      us_mi_COMS_Employment.Employment_Start_Date__raw,
      us_mi_COMS_Employment.Employment_End_Date__raw,
      us_mi_COMS_Employment.Reason_For_Leaving,
      us_mi_COMS_Employment.Employer_Aware_Of_Criminal_Record,
      us_mi_COMS_Employment.Entered_Date__raw,
      us_mi_COMS_Employment.file_id,
      us_mi_COMS_Employment.is_deleted]
    sorts: [us_mi_COMS_Employment.Employment_Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about JII employment information.  This table is active starting 8/14/2023 and should only hold employment information for JII employments that occur from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 102
    col: 0
    width: 24
    height: 6

  - name: COMS_Modifiers
    title: COMS_Modifiers
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Modifiers.primary_key,
      us_mi_COMS_Modifiers.Modifier_Id,
      us_mi_COMS_Modifiers.Supervision_Status_Id,
      us_mi_COMS_Modifiers.Offender_Number,
      us_mi_COMS_Modifiers.Modifier,
      us_mi_COMS_Modifiers.Start_Date__raw,
      us_mi_COMS_Modifiers.End_Date__raw,
      us_mi_COMS_Modifiers.Entered_Date__raw,
      us_mi_COMS_Modifiers.file_id,
      us_mi_COMS_Modifiers.is_deleted]
    sorts: [us_mi_COMS_Modifiers.Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about supervision sentence modifiers. Excludes entries associated with prison supervision status and inactive entries. This table is active starting 8/14/2023 and should only hold information that was  relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 108
    col: 0
    width: 24
    height: 6

  - name: COMS_Parole_Violation_Violation_Incidents
    title: COMS_Parole_Violation_Violation_Incidents
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Parole_Violation_Violation_Incidents.primary_key,
      us_mi_COMS_Parole_Violation_Violation_Incidents.Parole_Violation_Id,
      us_mi_COMS_Parole_Violation_Violation_Incidents.Offender_Number,
      us_mi_COMS_Parole_Violation_Violation_Incidents.Violation_Incident_Id,
      us_mi_COMS_Parole_Violation_Violation_Incidents.Entered_Date__raw,
      us_mi_COMS_Parole_Violation_Violation_Incidents.file_id,
      us_mi_COMS_Parole_Violation_Violation_Incidents.is_deleted]
    sorts: [us_mi_COMS_Parole_Violation_Violation_Incidents.Entered_Date__raw]
    note_display: hover
    note_text: "This table links parole violation records from COMS to supervision incident records from COMS"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 114
    col: 0
    width: 24
    height: 6

  - name: COMS_Parole_Violations
    title: COMS_Parole_Violations
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Parole_Violations.primary_key,
      us_mi_COMS_Parole_Violations.Parole_Violation_Id,
      us_mi_COMS_Parole_Violations.Offender_Number,
      us_mi_COMS_Parole_Violations.Case_Type,
      us_mi_COMS_Parole_Violations.Investigation_Start_Date__raw,
      us_mi_COMS_Parole_Violations.Investigation_Work_Unit,
      us_mi_COMS_Parole_Violations.Violation_Type,
      us_mi_COMS_Parole_Violations.Closed_Date__raw,
      us_mi_COMS_Parole_Violations.Entered_Date__raw,
      us_mi_COMS_Parole_Violations.file_id,
      us_mi_COMS_Parole_Violations.is_deleted]
    sorts: [us_mi_COMS_Parole_Violations.Investigation_Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the parole violations.  This table is active starting 8/14/2023 and should only hold parole violation information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 120
    col: 0
    width: 24
    height: 6

  - name: COMS_Probation_Violation_Violation_Incidents
    title: COMS_Probation_Violation_Violation_Incidents
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Probation_Violation_Violation_Incidents.primary_key,
      us_mi_COMS_Probation_Violation_Violation_Incidents.Probation_Violation_Id,
      us_mi_COMS_Probation_Violation_Violation_Incidents.Offender_Number,
      us_mi_COMS_Probation_Violation_Violation_Incidents.Violation_Incident_Id,
      us_mi_COMS_Probation_Violation_Violation_Incidents.Entered_Date__raw,
      us_mi_COMS_Probation_Violation_Violation_Incidents.file_id,
      us_mi_COMS_Probation_Violation_Violation_Incidents.is_deleted]
    sorts: [us_mi_COMS_Probation_Violation_Violation_Incidents.Entered_Date__raw]
    note_display: hover
    note_text: "This table links probation violation records from COMS to supervision incident records from COMS"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 126
    col: 0
    width: 24
    height: 6

  - name: COMS_Probation_Violations
    title: COMS_Probation_Violations
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Probation_Violations.primary_key,
      us_mi_COMS_Probation_Violations.Probation_Violation_Id,
      us_mi_COMS_Probation_Violations.Offender_Number,
      us_mi_COMS_Probation_Violations.Case_Type,
      us_mi_COMS_Probation_Violations.Investigation_Start_Date__raw,
      us_mi_COMS_Probation_Violations.Investigation_Work_Unit,
      us_mi_COMS_Probation_Violations.Due_Date__raw,
      us_mi_COMS_Probation_Violations.Probation_Violation_Sentence_Date__raw,
      us_mi_COMS_Probation_Violations.Recommendation,
      us_mi_COMS_Probation_Violations.Absconded_Date__raw,
      us_mi_COMS_Probation_Violations.Closed_Date__raw,
      us_mi_COMS_Probation_Violations.Closed_Reason,
      us_mi_COMS_Probation_Violations.Apprehended,
      us_mi_COMS_Probation_Violations.Apprehended_Date__raw,
      us_mi_COMS_Probation_Violations.Apprehension_Processed_By_Work_Unit,
      us_mi_COMS_Probation_Violations.Apprehension_Closed_Date__raw,
      us_mi_COMS_Probation_Violations.Apprehension_Closed_Reason,
      us_mi_COMS_Probation_Violations.Violation_Closed_Date__raw,
      us_mi_COMS_Probation_Violations.Entered_Date__raw,
      us_mi_COMS_Probation_Violations.file_id,
      us_mi_COMS_Probation_Violations.is_deleted]
    sorts: [us_mi_COMS_Probation_Violations.Investigation_Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the probation violations.  This table is active starting 8/14/2023 and should only hold probation violation information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 132
    col: 0
    width: 24
    height: 6

  - name: COMS_Program_Recommendations
    title: COMS_Program_Recommendations
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Program_Recommendations.primary_key,
      us_mi_COMS_Program_Recommendations.Program_Recommendation_Id,
      us_mi_COMS_Program_Recommendations.Offender_Number,
      us_mi_COMS_Program_Recommendations.Program,
      us_mi_COMS_Program_Recommendations.Create_Date__raw,
      us_mi_COMS_Program_Recommendations.Program_Status,
      us_mi_COMS_Program_Recommendations.Program_Status_Date__raw,
      us_mi_COMS_Program_Recommendations.Referral_Date__raw,
      us_mi_COMS_Program_Recommendations.Start_Date__raw,
      us_mi_COMS_Program_Recommendations.End_Date__raw,
      us_mi_COMS_Program_Recommendations.Program_End_Reason,
      us_mi_COMS_Program_Recommendations.Entered_Date__raw,
      us_mi_COMS_Program_Recommendations.file_id,
      us_mi_COMS_Program_Recommendations.is_deleted]
    sorts: [us_mi_COMS_Program_Recommendations.Create_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about program recommendations"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 138
    col: 0
    width: 24
    height: 6

  - name: COMS_Security_Classification
    title: COMS_Security_Classification
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Security_Classification.primary_key,
      us_mi_COMS_Security_Classification.Security_Classification_Id,
      us_mi_COMS_Security_Classification.Offender_Number,
      us_mi_COMS_Security_Classification.Confinement_Level_Assessment_Result,
      us_mi_COMS_Security_Classification.Confinement_Level_Assessment_Result_Entered_Date__raw,
      us_mi_COMS_Security_Classification.Management_Level_Assessment_Result,
      us_mi_COMS_Security_Classification.Management_Level_Assessment_Result_Entered_Date__raw,
      us_mi_COMS_Security_Classification.True_Security_Level_Assessment_Result,
      us_mi_COMS_Security_Classification.True_Security_Level_Assessment_Result_Entered_Date__raw,
      us_mi_COMS_Security_Classification.Actual_Placement_Level_Assessment_Result,
      us_mi_COMS_Security_Classification.Actual_Placement_Level_Assessment_Result_Entered_Date__raw,
      us_mi_COMS_Security_Classification.file_id,
      us_mi_COMS_Security_Classification.is_deleted]
    sorts: [us_mi_COMS_Security_Classification.Confinement_Level_Assessment_Result_Entered_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about security classification assessment results"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 144
    col: 0
    width: 24
    height: 6

  - name: COMS_Security_Standards_Toxin
    title: COMS_Security_Standards_Toxin
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Security_Standards_Toxin.primary_key,
      us_mi_COMS_Security_Standards_Toxin.Security_Standards_Toxin_Id,
      us_mi_COMS_Security_Standards_Toxin.Offender_Number,
      us_mi_COMS_Security_Standards_Toxin.Work_Unit,
      us_mi_COMS_Security_Standards_Toxin.Security_Standard_Toxin_Type,
      us_mi_COMS_Security_Standards_Toxin.Conducted_Date__raw,
      us_mi_COMS_Security_Standards_Toxin.Security_Standard_Reason,
      us_mi_COMS_Security_Standards_Toxin.Sample,
      us_mi_COMS_Security_Standards_Toxin.Initial_Test_Type,
      us_mi_COMS_Security_Standards_Toxin.Initial_Tested_By,
      us_mi_COMS_Security_Standards_Toxin.Initial_Outside_Vendor,
      us_mi_COMS_Security_Standards_Toxin.Lab_Sample_Reference_Code,
      us_mi_COMS_Security_Standards_Toxin.Result_Date__raw,
      us_mi_COMS_Security_Standards_Toxin.Retest_Test_Type,
      us_mi_COMS_Security_Standards_Toxin.Retest_By,
      us_mi_COMS_Security_Standards_Toxin.Retest_Outside_Vendor,
      us_mi_COMS_Security_Standards_Toxin.Retest_Sent_Date__raw,
      us_mi_COMS_Security_Standards_Toxin.Retest_Result_Date__raw,
      us_mi_COMS_Security_Standards_Toxin.Satisfactory,
      us_mi_COMS_Security_Standards_Toxin.Case_Manager_Response,
      us_mi_COMS_Security_Standards_Toxin.Case_Manager_Response_Date__raw,
      us_mi_COMS_Security_Standards_Toxin.Entered_Date__raw,
      us_mi_COMS_Security_Standards_Toxin.file_id,
      us_mi_COMS_Security_Standards_Toxin.is_deleted]
    sorts: [us_mi_COMS_Security_Standards_Toxin.Conducted_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about toxin screens.  This table is active starting 8/14/2023 and should only hold toxin screen information for JII employments that occur from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 150
    col: 0
    width: 24
    height: 6

  - name: COMS_Security_Threat_Group_Involvement
    title: COMS_Security_Threat_Group_Involvement
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Security_Threat_Group_Involvement.primary_key,
      us_mi_COMS_Security_Threat_Group_Involvement.Offender_Number,
      us_mi_COMS_Security_Threat_Group_Involvement.Security_Threat_Group,
      us_mi_COMS_Security_Threat_Group_Involvement.Security_Threat_Group_Status,
      us_mi_COMS_Security_Threat_Group_Involvement.Group_Leader,
      us_mi_COMS_Security_Threat_Group_Involvement.STG_Level,
      us_mi_COMS_Security_Threat_Group_Involvement.Start_Date__raw,
      us_mi_COMS_Security_Threat_Group_Involvement.Review_Date__raw,
      us_mi_COMS_Security_Threat_Group_Involvement.Entered_By,
      us_mi_COMS_Security_Threat_Group_Involvement.Entered_Date__raw,
      us_mi_COMS_Security_Threat_Group_Involvement.file_id,
      us_mi_COMS_Security_Threat_Group_Involvement.is_deleted]
    sorts: [us_mi_COMS_Security_Threat_Group_Involvement.Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about security threat groups."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 156
    col: 0
    width: 24
    height: 6

  - name: COMS_Specialties
    title: COMS_Specialties
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Specialties.primary_key,
      us_mi_COMS_Specialties.Specialty_Id,
      us_mi_COMS_Specialties.Supervision_Status_Id,
      us_mi_COMS_Specialties.Offender_Number,
      us_mi_COMS_Specialties.Specialty,
      us_mi_COMS_Specialties.Start_Date__raw,
      us_mi_COMS_Specialties.End_Date__raw,
      us_mi_COMS_Specialties.Entered_Date__raw,
      us_mi_COMS_Specialties.file_id,
      us_mi_COMS_Specialties.is_deleted]
    sorts: [us_mi_COMS_Specialties.Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about supervision specialties"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 162
    col: 0
    width: 24
    height: 6

  - name: COMS_Supervision_Levels
    title: COMS_Supervision_Levels
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Supervision_Levels.primary_key,
      us_mi_COMS_Supervision_Levels.Supervision_Level_Id,
      us_mi_COMS_Supervision_Levels.Supervision_Status_Id,
      us_mi_COMS_Supervision_Levels.Offender_Number,
      us_mi_COMS_Supervision_Levels.Supervision_Level,
      us_mi_COMS_Supervision_Levels.Start_Date__raw,
      us_mi_COMS_Supervision_Levels.End_Date__raw,
      us_mi_COMS_Supervision_Levels.Entered_Date__raw,
      us_mi_COMS_Supervision_Levels.file_id,
      us_mi_COMS_Supervision_Levels.is_deleted]
    sorts: [us_mi_COMS_Supervision_Levels.Start_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the supervision levels of JII on supervision.  This table is active starting 8/14/2023 and should only hold supervision level information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 168
    col: 0
    width: 24
    height: 6

  - name: COMS_Supervision_Schedule_Activities
    title: COMS_Supervision_Schedule_Activities
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Supervision_Schedule_Activities.primary_key,
      us_mi_COMS_Supervision_Schedule_Activities.Supervision_Schedule_Activity_Id,
      us_mi_COMS_Supervision_Schedule_Activities.Offender_Number,
      us_mi_COMS_Supervision_Schedule_Activities.Created_By_Source_Type,
      us_mi_COMS_Supervision_Schedule_Activities.Created_By_Source_Id,
      us_mi_COMS_Supervision_Schedule_Activities.Activity,
      us_mi_COMS_Supervision_Schedule_Activities.Scheduled_Date__raw,
      us_mi_COMS_Supervision_Schedule_Activities.Unnecessary,
      us_mi_COMS_Supervision_Schedule_Activities.Completed_Date__raw,
      us_mi_COMS_Supervision_Schedule_Activities.Completed_By_Staff_Omnni_Employee_Id,
      us_mi_COMS_Supervision_Schedule_Activities.Case_Note_Id,
      us_mi_COMS_Supervision_Schedule_Activities.Entered_Date__raw,
      us_mi_COMS_Supervision_Schedule_Activities.file_id,
      us_mi_COMS_Supervision_Schedule_Activities.is_deleted]
    sorts: [us_mi_COMS_Supervision_Schedule_Activities.Scheduled_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the supervision activities scheduled for each JII on supervision.  This table is active starting 8/14/2023 and should only hold supervision activity information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 174
    col: 0
    width: 24
    height: 6

  - name: COMS_Supervision_Schedules
    title: COMS_Supervision_Schedules
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Supervision_Schedules.primary_key,
      us_mi_COMS_Supervision_Schedules.Supervision_Schedule_Id,
      us_mi_COMS_Supervision_Schedules.Offender_Number,
      us_mi_COMS_Supervision_Schedules.Create_Date__raw,
      us_mi_COMS_Supervision_Schedules.Supervision_Activity_Panel,
      us_mi_COMS_Supervision_Schedules.Schedule_Start_Date__raw,
      us_mi_COMS_Supervision_Schedules.Schedule_End_Date__raw,
      us_mi_COMS_Supervision_Schedules.End_Date__raw,
      us_mi_COMS_Supervision_Schedules.Entered_Date__raw,
      us_mi_COMS_Supervision_Schedules.file_id,
      us_mi_COMS_Supervision_Schedules.is_deleted]
    sorts: [us_mi_COMS_Supervision_Schedules.Create_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the supervision schedule for each JII on supervision.  This table is active starting 8/14/2023 and should only hold supervision schedule information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 180
    col: 0
    width: 24
    height: 6

  - name: COMS_Violation_Incident_Charges
    title: COMS_Violation_Incident_Charges
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Violation_Incident_Charges.primary_key,
      us_mi_COMS_Violation_Incident_Charges.Violation_Incident_Charge_Id,
      us_mi_COMS_Violation_Incident_Charges.Violation_Incident_Id,
      us_mi_COMS_Violation_Incident_Charges.Offender_Number,
      us_mi_COMS_Violation_Incident_Charges.Technical_Non_Technical,
      us_mi_COMS_Violation_Incident_Charges.Disposition,
      us_mi_COMS_Violation_Incident_Charges.Entered_Date__raw,
      us_mi_COMS_Violation_Incident_Charges.file_id,
      us_mi_COMS_Violation_Incident_Charges.is_deleted]
    sorts: [us_mi_COMS_Violation_Incident_Charges.Entered_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the supervision violation incident charges."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 186
    col: 0
    width: 24
    height: 6

  - name: COMS_Violation_Incidents
    title: COMS_Violation_Incidents
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_COMS_Violation_Incidents.primary_key,
      us_mi_COMS_Violation_Incidents.Violation_Incident_Id,
      us_mi_COMS_Violation_Incidents.Offender_Number,
      us_mi_COMS_Violation_Incidents.Incident_Type,
      us_mi_COMS_Violation_Incidents.Incident_Date__raw,
      us_mi_COMS_Violation_Incidents.Absconded,
      us_mi_COMS_Violation_Incidents.Absconded_Date__raw,
      us_mi_COMS_Violation_Incidents.Absconded_From_Work_Unit,
      us_mi_COMS_Violation_Incidents.Apprehended_Date__raw,
      us_mi_COMS_Violation_Incidents.Arrested,
      us_mi_COMS_Violation_Incidents.Entered_Date__raw,
      us_mi_COMS_Violation_Incidents.file_id,
      us_mi_COMS_Violation_Incidents.is_deleted]
    sorts: [us_mi_COMS_Violation_Incidents.Incident_Date__raw]
    note_display: hover
    note_text: "This table includes data from the COMS system about the supervision violation incidents (where supervision violation incidents ladder up into a supervision violation).  This table is active starting 8/14/2023 and should only hold supervision violation incident information that was relevant from 8/14/2023 onwards."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 192
    col: 0
    width: 24
    height: 6

  - name: Table_Data_TRANSCASE_FORMS_Prod
    title: Table_Data_TRANSCASE_FORMS_Prod
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_Table_Data_TRANSCASE_FORMS_Prod.primary_key,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.OffenderNumber,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.FinalizedDate__raw,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.DecisionCategory,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.ConfinementLevel,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.ManagementLevel,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.TrueSecurityLevel,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.ActualPlacementLevel,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.WaiverReason,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.DepartureReason,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.file_id,
      us_mi_Table_Data_TRANSCASE_FORMS_Prod.is_deleted]
    sorts: [us_mi_Table_Data_TRANSCASE_FORMS_Prod.FinalizedDate__raw]
    note_display: hover
    note_text: "Table containing historical security classification screens data with overrides"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 198
    col: 0
    width: 24
    height: 6

  - name: ADH_CASE_NOTE_DETAIL
    title: ADH_CASE_NOTE_DETAIL
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_CASE_NOTE_DETAIL.primary_key,
      us_mi_ADH_CASE_NOTE_DETAIL.case_note_detail_id,
      us_mi_ADH_CASE_NOTE_DETAIL.offender_booking_id,
      us_mi_ADH_CASE_NOTE_DETAIL.sequence_number,
      us_mi_ADH_CASE_NOTE_DETAIL.offender_schedule_id,
      us_mi_ADH_CASE_NOTE_DETAIL.event_date__raw,
      us_mi_ADH_CASE_NOTE_DETAIL.schedule_type_id,
      us_mi_ADH_CASE_NOTE_DETAIL.schedule_reason_id,
      us_mi_ADH_CASE_NOTE_DETAIL.recorded_date__raw,
      us_mi_ADH_CASE_NOTE_DETAIL.notes,
      us_mi_ADH_CASE_NOTE_DETAIL.notes2,
      us_mi_ADH_CASE_NOTE_DETAIL.notes3,
      us_mi_ADH_CASE_NOTE_DETAIL.notes4,
      us_mi_ADH_CASE_NOTE_DETAIL.notes5,
      us_mi_ADH_CASE_NOTE_DETAIL.notes6,
      us_mi_ADH_CASE_NOTE_DETAIL.notes7,
      us_mi_ADH_CASE_NOTE_DETAIL.case_note_detail_text_id,
      us_mi_ADH_CASE_NOTE_DETAIL.last_update_user,
      us_mi_ADH_CASE_NOTE_DETAIL.last_update_date__raw,
      us_mi_ADH_CASE_NOTE_DETAIL.last_update_node,
      us_mi_ADH_CASE_NOTE_DETAIL.web_publish_flag,
      us_mi_ADH_CASE_NOTE_DETAIL.record_source_id,
      us_mi_ADH_CASE_NOTE_DETAIL.program_id,
      us_mi_ADH_CASE_NOTE_DETAIL.file_id,
      us_mi_ADH_CASE_NOTE_DETAIL.is_deleted]
    sorts: [us_mi_ADH_CASE_NOTE_DETAIL.event_date__raw]
    note_display: hover
    note_text: "OMNI database table that contains case note details"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 204
    col: 0
    width: 24
    height: 6

  - name: ADH_EMC_WARRANT
    title: ADH_EMC_WARRANT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_EMC_WARRANT.primary_key,
      us_mi_ADH_EMC_WARRANT.emc_warrant_id,
      us_mi_ADH_EMC_WARRANT.offender_booking_id,
      us_mi_ADH_EMC_WARRANT.warrant_date__raw,
      us_mi_ADH_EMC_WARRANT.entry_operator_user_id,
      us_mi_ADH_EMC_WARRANT.entry_authorized_by,
      us_mi_ADH_EMC_WARRANT.entry_authorized_employee_id,
      us_mi_ADH_EMC_WARRANT.offender_status_id,
      us_mi_ADH_EMC_WARRANT.offender_type_id,
      us_mi_ADH_EMC_WARRANT.agent_employee_id,
      us_mi_ADH_EMC_WARRANT.agent_caseload_number,
      us_mi_ADH_EMC_WARRANT.entry_aru_referral_date__raw,
      us_mi_ADH_EMC_WARRANT.entry_aru_referral_user_id,
      us_mi_ADH_EMC_WARRANT.cancel_operator_user_id,
      us_mi_ADH_EMC_WARRANT.cancel_date__raw,
      us_mi_ADH_EMC_WARRANT.cancel_authorized_by,
      us_mi_ADH_EMC_WARRANT.cancel_authorized_employee_id,
      us_mi_ADH_EMC_WARRANT.cancel_location_id,
      us_mi_ADH_EMC_WARRANT.cancel_aru_closing_date__raw,
      us_mi_ADH_EMC_WARRANT.cancel_aru_closing_reason_id,
      us_mi_ADH_EMC_WARRANT.cancel_aru_closing_user_id,
      us_mi_ADH_EMC_WARRANT.warrant_entry_date,
      us_mi_ADH_EMC_WARRANT.escape_abscond_location_id,
      us_mi_ADH_EMC_WARRANT.entry_remarks,
      us_mi_ADH_EMC_WARRANT.entry_aru_remarks,
      us_mi_ADH_EMC_WARRANT.cancel_remarks,
      us_mi_ADH_EMC_WARRANT.cancel_aru_remarks,
      us_mi_ADH_EMC_WARRANT.last_update_user,
      us_mi_ADH_EMC_WARRANT.last_update_date__raw,
      us_mi_ADH_EMC_WARRANT.last_update_node,
      us_mi_ADH_EMC_WARRANT.file_id,
      us_mi_ADH_EMC_WARRANT.is_deleted]
    sorts: [us_mi_ADH_EMC_WARRANT.warrant_date__raw]
    note_display: hover
    note_text: "This table contains information on warrants issued for people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 210
    col: 0
    width: 24
    height: 6

  - name: ADH_EMPLOYEE_BOOKING_ASSIGNMENT
    title: ADH_EMPLOYEE_BOOKING_ASSIGNMENT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.primary_key,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.offender_booking_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.employee_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.sequence_number,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_date__raw,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.closure_date__raw,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_type_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.active_flag,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_employee_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.source_employee_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.destination_employee_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.notes,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_subtype_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.closing_reason_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.investigation_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_location_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_case_load_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.workload_number,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.temporary_assignment,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.last_update_user,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.last_update_date,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.last_update_node,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.file_id,
      us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.is_deleted]
    sorts: [us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.assignment_date__raw]
    note_display: hover
    note_text: "Table containing employees assigned to oversee people under a given jurisdiction in MIDOC."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 216
    col: 0
    width: 24
    height: 6

  - name: ADH_LEGAL_ORDER
    title: ADH_LEGAL_ORDER
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_LEGAL_ORDER.primary_key,
      us_mi_ADH_LEGAL_ORDER.legal_order_id,
      us_mi_ADH_LEGAL_ORDER.offender_booking_id,
      us_mi_ADH_LEGAL_ORDER.order_type_id,
      us_mi_ADH_LEGAL_ORDER.order_status_id,
      us_mi_ADH_LEGAL_ORDER.create_date__raw,
      us_mi_ADH_LEGAL_ORDER.issue_date__raw,
      us_mi_ADH_LEGAL_ORDER.issue_location_id,
      us_mi_ADH_LEGAL_ORDER.issue_reason_code,
      us_mi_ADH_LEGAL_ORDER.county_code_id,
      us_mi_ADH_LEGAL_ORDER.released_from_location_id,
      us_mi_ADH_LEGAL_ORDER.notes,
      us_mi_ADH_LEGAL_ORDER.effective_date__raw,
      us_mi_ADH_LEGAL_ORDER.expiration_date__raw,
      us_mi_ADH_LEGAL_ORDER.attending_court_id,
      us_mi_ADH_LEGAL_ORDER.court_date,
      us_mi_ADH_LEGAL_ORDER.next_court_date,
      us_mi_ADH_LEGAL_ORDER.review_date,
      us_mi_ADH_LEGAL_ORDER.youth_adult_code,
      us_mi_ADH_LEGAL_ORDER.suspended_date,
      us_mi_ADH_LEGAL_ORDER.suspended_parole_id,
      us_mi_ADH_LEGAL_ORDER.closing_reason_code,
      us_mi_ADH_LEGAL_ORDER.closing_authority_id,
      us_mi_ADH_LEGAL_ORDER.closing_date__raw,
      us_mi_ADH_LEGAL_ORDER.closing_notes,
      us_mi_ADH_LEGAL_ORDER.offender_court_last_name,
      us_mi_ADH_LEGAL_ORDER.offender_court_first_name,
      us_mi_ADH_LEGAL_ORDER.offender_court_middle_name,
      us_mi_ADH_LEGAL_ORDER.offender_court_name_suffix,
      us_mi_ADH_LEGAL_ORDER.reporting_instructions,
      us_mi_ADH_LEGAL_ORDER.order_disclosure_id,
      us_mi_ADH_LEGAL_ORDER.prison_prefix,
      us_mi_ADH_LEGAL_ORDER.judge_id,
      us_mi_ADH_LEGAL_ORDER.defense_counsel_name,
      us_mi_ADH_LEGAL_ORDER.prosecuting_attorney_name,
      us_mi_ADH_LEGAL_ORDER.retained_appointed_id,
      us_mi_ADH_LEGAL_ORDER.sentencing_employee_id,
      us_mi_ADH_LEGAL_ORDER.sentencing_location_id,
      us_mi_ADH_LEGAL_ORDER.docket_number_description,
      us_mi_ADH_LEGAL_ORDER.last_update_user,
      us_mi_ADH_LEGAL_ORDER.last_update_date,
      us_mi_ADH_LEGAL_ORDER.last_update_node,
      us_mi_ADH_LEGAL_ORDER.file_id,
      us_mi_ADH_LEGAL_ORDER.is_deleted]
    sorts: [us_mi_ADH_LEGAL_ORDER.create_date__raw]
    note_display: hover
    note_text: "This table contains all legal orders for anyone who is called to court within MIDOC."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 222
    col: 0
    width: 24
    height: 6

  - name: ADH_MISCONDUCT_INCIDENT
    title: ADH_MISCONDUCT_INCIDENT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_MISCONDUCT_INCIDENT.primary_key,
      us_mi_ADH_MISCONDUCT_INCIDENT.misconduct_incident_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.offender_booking_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.location_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.miscond_status_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.miscond_violation_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.miscond_written_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.reporting_employee_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.incident_place_type_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.cmis_record_number,
      us_mi_ADH_MISCONDUCT_INCIDENT.housing_unit,
      us_mi_ADH_MISCONDUCT_INCIDENT.cell_bunk_room,
      us_mi_ADH_MISCONDUCT_INCIDENT.reporting_clock_no,
      us_mi_ADH_MISCONDUCT_INCIDENT.pending_hearing_status_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.pending_hearing_status_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.review_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.review_employee_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.post_review_status_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.post_review_status_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.hearing_invest_request_flag,
      us_mi_ADH_MISCONDUCT_INCIDENT.relevant_doc_requested_flag,
      us_mi_ADH_MISCONDUCT_INCIDENT.waive_24hour_flag,
      us_mi_ADH_MISCONDUCT_INCIDENT.incident_finding_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.incident_location_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.notified_employee,
      us_mi_ADH_MISCONDUCT_INCIDENT.witness_requested_flag,
      us_mi_ADH_MISCONDUCT_INCIDENT.proposed_hearing_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.proposed_hearing_location_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.last_date_for_hearing__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.last_update_user,
      us_mi_ADH_MISCONDUCT_INCIDENT.last_update_date__raw,
      us_mi_ADH_MISCONDUCT_INCIDENT.last_update_node,
      us_mi_ADH_MISCONDUCT_INCIDENT.file_id,
      us_mi_ADH_MISCONDUCT_INCIDENT.is_deleted]
    sorts: [us_mi_ADH_MISCONDUCT_INCIDENT.miscond_violation_date__raw]
    note_display: hover
    note_text: "OMNI table that records misconduct incidents"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 228
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_ASSESSMENT
    title: ADH_OFFENDER_ASSESSMENT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_ASSESSMENT.primary_key,
      us_mi_ADH_OFFENDER_ASSESSMENT.offender_assessment_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.offender_booking_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.decision_type_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.decision_category_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.calculated_score,
      us_mi_ADH_OFFENDER_ASSESSMENT.assessment_result_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.assessment_date__raw,
      us_mi_ADH_OFFENDER_ASSESSMENT.calculated_result_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.override_result_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.override_employee_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.investigation_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.next_review_date,
      us_mi_ADH_OFFENDER_ASSESSMENT.prepared_by_employee_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.court_file_no,
      us_mi_ADH_OFFENDER_ASSESSMENT.notes,
      us_mi_ADH_OFFENDER_ASSESSMENT.last_update_user,
      us_mi_ADH_OFFENDER_ASSESSMENT.last_update_date__raw,
      us_mi_ADH_OFFENDER_ASSESSMENT.last_update_node,
      us_mi_ADH_OFFENDER_ASSESSMENT.file_id,
      us_mi_ADH_OFFENDER_ASSESSMENT.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_ASSESSMENT.assessment_date__raw]
    note_display: hover
    note_text: "This table contains assessment information for people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 234
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_BASIC_INFO_104A
    title: ADH_OFFENDER_BASIC_INFO_104A
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_BASIC_INFO_104A.primary_key,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.offender_booking_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pending_charge_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hold_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.probat_violation_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hist_of_arson,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hist_of_drug_sales,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hist_of_assault,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hist_of_sex_offense_minor,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hist_of_sex_offense,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.history_srce_code_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.history_srce_code_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.history_srce_code_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.history_srce_code_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.phys_prob_complaint_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.phys_prob_complaint_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.phys_prob_complaint_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.phys_prob_complaint_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.special_diet_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.medication_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.special_diet_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.special_diet_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.special_diet_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.special_diet_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.sever_psych_problem_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hospitalization_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_past_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_present_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_srce_t,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.psych_hosp_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_potential_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.chronic_depression_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_past,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_present,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_srce_t,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.suicide_depression_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.intellect_deficiency_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.intellect_deficiency_srce_t,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.intellect_deficiency_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.intellect_deficiency_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.intellect_deficiency_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_community_prog_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_history_close_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_history_med_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_history_min_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_comm_prog_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_comm_prog_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_comm_prog_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_comm_prog_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_juv_inst_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_juv_inst_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_juv_inst_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_juv_inst_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_juv_inst_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_awol_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_awol_within_yrs_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_awol_within_yrs_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_awol_within_yrs_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_awol_within_yrs_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.viol_crime_inj_death_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.violent_crime_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.violent_crime_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.assaultive_pattern_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.assaultive_pattern_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.assaultive_pattern_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.assaultive_pattern_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.assaultive_pattern_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.belligerent_to_authority_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.bellig_to_authority_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.bellig_to_authority_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.bellig_to_authority_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.bellig_to_authority_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_behav_hist_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_behav_hist_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_behav_hist_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_behav_hist_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_behav_hist_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_behav_hist_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.two_violations_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.two_violations_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.two_violations_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.two_violations_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.two_violations_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.deficient_cop_skills_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.deficient_cop_skills_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.deficient_cop_skills_srce_t,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.deficient_cop_skills_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.deficient_cop_skills_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.deficient_cop_skills_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.adult_record_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.adult_record_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.adult_record_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.adult_record_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_before_15_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_commit_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_probat_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.juvenile_rec_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.married_prior_offense_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.current_marital_status_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.never_married_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.marital_rec_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.marital_rec_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.marital_rec_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.marital_rec_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.instant_offense_assaultive,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.very_high_risk_notice_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.very_high_risk_notice_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.very_high_risk_notice_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.very_high_risk_notice_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.notice_high_risk_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.effective_date__raw,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.to_location_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.recommend_by_emp_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.security_level,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pending_charge_comments,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.offense,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.docket_no,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.jurisdiction,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.max_time,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.offense_1,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.docket_no_1,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.jurisdiction_1,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.max_time_1,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_specify,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_other_within_yrs,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_comm_prog_within_yrs,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_juv_inst_within_yrs,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_awol_within_yrs,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_history_comments,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.homosex_designated_date__raw,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.num_prior_prison_terms,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.num_prior_adult_probations,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.age_1st_juvenile_arrest,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.bi_placement_comments,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.med_psych_none_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.escape_hist_none_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.adj_prob_none_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.last_update_user,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.last_update_date__raw,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.last_update_node,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hods_srce_code_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hods_srce_code_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hods_srce_code_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hods_srce_code_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoa_srce_code_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoa_srce_code_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoa_srce_code_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoa_srce_code_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoso_srce_code_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoso_srce_code_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoso_srce_code_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hoso_srce_code_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hosom_srce_code_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hosom_srce_code_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hosom_srce_code_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.hosom_srce_code_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pregnant_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pf_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pf_srce_t,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pf_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pf_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.pf_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.mf_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.mf_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.mf_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.mf_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_past,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_present,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_srce_i,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_srce_t,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_srce_p,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_srce_ir,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.cd_srce_cr,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.phys_prob_complaint_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.intellect_deficiency_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.compas_non_vfo_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.static_risk_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.compas_screener_employee_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.static_risk_screener_emp_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.compass_screen_date__raw,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.static_risk_screen_date__raw,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.additional_offense_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.additional_off_flag,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.domestic_violence_cnvt,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.general_violence_cnvt,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.compas_vfo_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.gv_domestic_cnvt,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.file_id,
      us_mi_ADH_OFFENDER_BASIC_INFO_104A.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_BASIC_INFO_104A.effective_date__raw]
    note_display: hover
    note_text: "This table contains basic summary information for people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 240
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_BOOKING_PROFILE
    title: ADH_OFFENDER_BOOKING_PROFILE
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_BOOKING_PROFILE.primary_key,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.offender_booking_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.profile_date__raw,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.living_unit_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.eye_color_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.hair_color_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.racial_appearance_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.cultural_affiliation_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.multiracial_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.caucasian_racial_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.native_american_racial_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.black_racial_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.asian_racial_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.skin_complexion_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.gender_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.marital_status_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.facial_hair_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.baldness_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.citizenship_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.dexterity_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.build_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.weight,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.height_feet,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.height_inches,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.eye_glasses_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.eye_contacts_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.known_homosexual_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.dependents_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.juvenile_commitments_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.juvenile_probation_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.juvenile_escape_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.adult_jail_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.adult_prison_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.adult_probation_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.adult_escape_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.csc_convictions_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.first_arrest_age,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.prior_felony_conviction_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.year_entered_country,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.pending_charge_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.cmis_perm_lock,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.cmis_temp_lock,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.have_health_insurance_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.prior_misdem_convict_count,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.hispanic_flag,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.racial_identification_code_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.start_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.end_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.profile_closing_reason_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.medical_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.medical_release_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.psychological_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.psychological_release_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.dental_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.dental_release_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.mental_health_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.mental_health_release_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.sa_file_review_employee_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.sa_file_review_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.sa_file_review_result_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.notes,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.last_update_user,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.last_update_date,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.last_update_node,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.file_id,
      us_mi_ADH_OFFENDER_BOOKING_PROFILE.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_BOOKING_PROFILE.profile_date__raw]
    note_display: hover
    note_text: "This table indicates the profile associated with a person and their booking within MDOC, which represents a jurisdiction or stint of being involved within MDOC."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 246
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_BOOKING_REPORT
    title: ADH_OFFENDER_BOOKING_REPORT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_BOOKING_REPORT.primary_key,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.offender_booking_id,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.related_report_id,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.last_update_user,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.last_update_date__raw,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.last_update_node,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.file_id,
      us_mi_ADH_OFFENDER_BOOKING_REPORT.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_BOOKING_REPORT.last_update_date__raw]
    note_display: hover
    note_text: "This table is links between offender booking and reports."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 252
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_CHARGE
    title: ADH_OFFENDER_CHARGE
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_CHARGE.primary_key,
      us_mi_ADH_OFFENDER_CHARGE.offender_charge_id,
      us_mi_ADH_OFFENDER_CHARGE.offender_booking_id,
      us_mi_ADH_OFFENDER_CHARGE.legal_order_id,
      us_mi_ADH_OFFENDER_CHARGE.charge_number,
      us_mi_ADH_OFFENDER_CHARGE.offense_id,
      us_mi_ADH_OFFENDER_CHARGE.charge_status_id,
      us_mi_ADH_OFFENDER_CHARGE.court_file_no,
      us_mi_ADH_OFFENDER_CHARGE.offense_date__raw,
      us_mi_ADH_OFFENDER_CHARGE.conviction_date__raw,
      us_mi_ADH_OFFENDER_CHARGE.conviction_type_id,
      us_mi_ADH_OFFENDER_CHARGE.convict_investigation_type_id,
      us_mi_ADH_OFFENDER_CHARGE.firearms_involved_flag,
      us_mi_ADH_OFFENDER_CHARGE.attempted_id,
      us_mi_ADH_OFFENDER_CHARGE.charge_bond_bail_amount,
      us_mi_ADH_OFFENDER_CHARGE.bond_bail_type_id,
      us_mi_ADH_OFFENDER_CHARGE.bond_bail_status_id,
      us_mi_ADH_OFFENDER_CHARGE.bond_bail_date,
      us_mi_ADH_OFFENDER_CHARGE.criminal_tracking_number,
      us_mi_ADH_OFFENDER_CHARGE.date_of_arrest__raw,
      us_mi_ADH_OFFENDER_CHARGE.guilty_but_mentally_ill_flag,
      us_mi_ADH_OFFENDER_CHARGE.jail_credit_days,
      us_mi_ADH_OFFENDER_CHARGE.habitual_override_flag,
      us_mi_ADH_OFFENDER_CHARGE.enhanced_offense_id,
      us_mi_ADH_OFFENDER_CHARGE.docket_number_lookup,
      us_mi_ADH_OFFENDER_CHARGE.closing_reason_code,
      us_mi_ADH_OFFENDER_CHARGE.closing_autohrity_id,
      us_mi_ADH_OFFENDER_CHARGE.closing_date__raw,
      us_mi_ADH_OFFENDER_CHARGE.closing_notes,
      us_mi_ADH_OFFENDER_CHARGE.last_update_user,
      us_mi_ADH_OFFENDER_CHARGE.last_update_date__raw,
      us_mi_ADH_OFFENDER_CHARGE.last_update_node,
      us_mi_ADH_OFFENDER_CHARGE.file_id,
      us_mi_ADH_OFFENDER_CHARGE.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_CHARGE.offense_date__raw]
    note_display: hover
    note_text: "This table contains all information about the charges against a person."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 258
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_EMPLOYMENT
    title: ADH_OFFENDER_EMPLOYMENT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_EMPLOYMENT.primary_key,
      us_mi_ADH_OFFENDER_EMPLOYMENT.offender_booking_id,
      us_mi_ADH_OFFENDER_EMPLOYMENT.sequence_number,
      us_mi_ADH_OFFENDER_EMPLOYMENT.employment_date__raw,
      us_mi_ADH_OFFENDER_EMPLOYMENT.employment_mo_yr_flag,
      us_mi_ADH_OFFENDER_EMPLOYMENT.employment_yr_flag,
      us_mi_ADH_OFFENDER_EMPLOYMENT.termination_date__raw,
      us_mi_ADH_OFFENDER_EMPLOYMENT.termination_mo_yr_flag,
      us_mi_ADH_OFFENDER_EMPLOYMENT.termination_yr_flag,
      us_mi_ADH_OFFENDER_EMPLOYMENT.employer_name,
      us_mi_ADH_OFFENDER_EMPLOYMENT.supervisor_name,
      us_mi_ADH_OFFENDER_EMPLOYMENT.position,
      us_mi_ADH_OFFENDER_EMPLOYMENT.employment_status_id,
      us_mi_ADH_OFFENDER_EMPLOYMENT.termination_reason_code,
      us_mi_ADH_OFFENDER_EMPLOYMENT.wage,
      us_mi_ADH_OFFENDER_EMPLOYMENT.wage_period_code,
      us_mi_ADH_OFFENDER_EMPLOYMENT.address1,
      us_mi_ADH_OFFENDER_EMPLOYMENT.address2,
      us_mi_ADH_OFFENDER_EMPLOYMENT.address3,
      us_mi_ADH_OFFENDER_EMPLOYMENT.city,
      us_mi_ADH_OFFENDER_EMPLOYMENT.state_id,
      us_mi_ADH_OFFENDER_EMPLOYMENT.postal_code,
      us_mi_ADH_OFFENDER_EMPLOYMENT.country_id,
      us_mi_ADH_OFFENDER_EMPLOYMENT.business_phone_country_code,
      us_mi_ADH_OFFENDER_EMPLOYMENT.business_phone_area_code,
      us_mi_ADH_OFFENDER_EMPLOYMENT.business_phone_number,
      us_mi_ADH_OFFENDER_EMPLOYMENT.business_phone_extension,
      us_mi_ADH_OFFENDER_EMPLOYMENT.business_fax_area_code,
      us_mi_ADH_OFFENDER_EMPLOYMENT.business_fax_number,
      us_mi_ADH_OFFENDER_EMPLOYMENT.notes,
      us_mi_ADH_OFFENDER_EMPLOYMENT.work_schedule,
      us_mi_ADH_OFFENDER_EMPLOYMENT.employer_knows_record_flag,
      us_mi_ADH_OFFENDER_EMPLOYMENT.occupation_description,
      us_mi_ADH_OFFENDER_EMPLOYMENT.occupation,
      us_mi_ADH_OFFENDER_EMPLOYMENT.last_update_user,
      us_mi_ADH_OFFENDER_EMPLOYMENT.last_update_date__raw,
      us_mi_ADH_OFFENDER_EMPLOYMENT.last_update_node,
      us_mi_ADH_OFFENDER_EMPLOYMENT.file_id,
      us_mi_ADH_OFFENDER_EMPLOYMENT.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_EMPLOYMENT.employment_date__raw]
    note_display: hover
    note_text: "Table recording employment for justice-involved individuals in the MDOC system"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 264
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_EXTERNAL_MOVEMENT
    title: ADH_OFFENDER_EXTERNAL_MOVEMENT
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.primary_key,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.offender_external_movement_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.offender_booking_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.source_location_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.movement_date__raw,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.destination_location_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.reporting_date__raw,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.movement_reason_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.escort_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.escort_notes,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.notes,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.active_flag,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.last_update_user,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.last_update_date__raw,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.last_update_node,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.file_id,
      us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.movement_date__raw]
    note_display: hover
    note_text: "This table records every single movement that a justice-involved individual makes within MDOC, such as transfers between facilities, transfers to parole, readmissions, etc."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 270
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_FEE_PROFILE
    title: ADH_OFFENDER_FEE_PROFILE
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_FEE_PROFILE.primary_key,
      us_mi_ADH_OFFENDER_FEE_PROFILE.offender_fee_profile_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.cos_fee_type_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.offender_booking_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.profile_create_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.initial_amount_ordered,
      us_mi_ADH_OFFENDER_FEE_PROFILE.total_amount_ordered,
      us_mi_ADH_OFFENDER_FEE_PROFILE.months,
      us_mi_ADH_OFFENDER_FEE_PROFILE.amount_paid_todate,
      us_mi_ADH_OFFENDER_FEE_PROFILE.last_payment_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.other_assets_flag,
      us_mi_ADH_OFFENDER_FEE_PROFILE.court_file_no,
      us_mi_ADH_OFFENDER_FEE_PROFILE.status_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.fee_review_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.fee_review_notes,
      us_mi_ADH_OFFENDER_FEE_PROFILE.review_employee_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.supv_discharge_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.closing_authority_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.closing_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.closing_notes,
      us_mi_ADH_OFFENDER_FEE_PROFILE.closing_reason_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.create_location_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.no_order_zero_order_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.final_assessment_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.treasury_hold_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.statement_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.warning_90_day_letter_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.profile_notes,
      us_mi_ADH_OFFENDER_FEE_PROFILE.last_update_user,
      us_mi_ADH_OFFENDER_FEE_PROFILE.last_update_date__raw,
      us_mi_ADH_OFFENDER_FEE_PROFILE.last_update_node,
      us_mi_ADH_OFFENDER_FEE_PROFILE.file_id,
      us_mi_ADH_OFFENDER_FEE_PROFILE.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_FEE_PROFILE.profile_create_date__raw]
    note_display: hover
    note_text: "MDOC table recording offender fee profiles"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 276
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_MISCOND_HEARING
    title: ADH_OFFENDER_MISCOND_HEARING
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_MISCOND_HEARING.primary_key,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.offender_miscond_hearing_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.offender_booking_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.misconduct_incident_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.location_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.hearing_type_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.hearing_date__raw,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.hearing_by_employee_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.notice_delivery_date__raw,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.refuse_to_sign_flag,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.notice_delivery_employee_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.report_read_flag,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.investigation_rep_read_flag,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.hearing_report_delivery_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.closing_authority_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.closing_date__raw,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.closing_notes,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.closing_reason_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.hearing_finding_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.last_update_user,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.last_update_date__raw,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.last_update_node,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.file_id,
      us_mi_ADH_OFFENDER_MISCOND_HEARING.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_MISCOND_HEARING.hearing_date__raw]
    note_display: hover
    note_text: "OMNI table recording misconduct hearings for justice-involved individuals"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 282
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_RGC_RECOMMENDATION
    title: ADH_OFFENDER_RGC_RECOMMENDATION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.primary_key,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.offender_rgc_recommendation_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.offender_booking_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.rgc_requirement_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.rgc_requirement_type,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.create_date__raw,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.pb_approval_flag,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.last_update_user,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.last_update_date__raw,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.last_update_node,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.recommendation_origin_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.pb_status_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.offender_rgc_master_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.file_id,
      us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.create_date__raw]
    note_display: hover
    note_text: "OMNI table that records RGC (Egeler Reception and Guidance Center) recommendations"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 288
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_RGC_TRACKING
    title: ADH_OFFENDER_RGC_TRACKING
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_RGC_TRACKING.primary_key,
      us_mi_ADH_OFFENDER_RGC_TRACKING.offender_rgc_tracking_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.offender_booking_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.offender_rgc_recommendation_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.rgc_status_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.rgc_status_date__raw,
      us_mi_ADH_OFFENDER_RGC_TRACKING.referral_date__raw,
      us_mi_ADH_OFFENDER_RGC_TRACKING.start_date__raw,
      us_mi_ADH_OFFENDER_RGC_TRACKING.create_date__raw,
      us_mi_ADH_OFFENDER_RGC_TRACKING.created_by_empl_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.rgc_rational_code_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.rgc_note_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.location_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.termination_reason_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.termination_date__raw,
      us_mi_ADH_OFFENDER_RGC_TRACKING.authorized_by_empl,
      us_mi_ADH_OFFENDER_RGC_TRACKING.notes,
      us_mi_ADH_OFFENDER_RGC_TRACKING.last_update_user,
      us_mi_ADH_OFFENDER_RGC_TRACKING.last_update_date__raw,
      us_mi_ADH_OFFENDER_RGC_TRACKING.last_update_node,
      us_mi_ADH_OFFENDER_RGC_TRACKING.file_id,
      us_mi_ADH_OFFENDER_RGC_TRACKING.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_RGC_TRACKING.rgc_status_date__raw]
    note_display: hover
    note_text: "OMNI table that tracks the status of RGC (Egeler Reception and Guidance Center) recommendations"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 294
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_SCHEDULE
    title: ADH_OFFENDER_SCHEDULE
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_SCHEDULE.primary_key,
      us_mi_ADH_OFFENDER_SCHEDULE.offender_schedule_id,
      us_mi_ADH_OFFENDER_SCHEDULE.offender_booking_id,
      us_mi_ADH_OFFENDER_SCHEDULE.schedule_type_id,
      us_mi_ADH_OFFENDER_SCHEDULE.schedule_reason_id,
      us_mi_ADH_OFFENDER_SCHEDULE.event_date__raw,
      us_mi_ADH_OFFENDER_SCHEDULE.direction_code_id,
      us_mi_ADH_OFFENDER_SCHEDULE.scheduled_location_code,
      us_mi_ADH_OFFENDER_SCHEDULE.scheduled_return_date__raw,
      us_mi_ADH_OFFENDER_SCHEDULE.active_flag,
      us_mi_ADH_OFFENDER_SCHEDULE.waiver_signed_flag,
      us_mi_ADH_OFFENDER_SCHEDULE.create_date__raw,
      us_mi_ADH_OFFENDER_SCHEDULE.notes,
      us_mi_ADH_OFFENDER_SCHEDULE.item_complete_date__raw,
      us_mi_ADH_OFFENDER_SCHEDULE.person_id,
      us_mi_ADH_OFFENDER_SCHEDULE.case_note_detail_id,
      us_mi_ADH_OFFENDER_SCHEDULE.last_update_user,
      us_mi_ADH_OFFENDER_SCHEDULE.last_update_date__raw,
      us_mi_ADH_OFFENDER_SCHEDULE.last_update_node,
      us_mi_ADH_OFFENDER_SCHEDULE.file_id,
      us_mi_ADH_OFFENDER_SCHEDULE.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_SCHEDULE.event_date__raw]
    note_display: hover
    note_text: "OMNI table that records tasks that should be completed for each JII in the MDOC system"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 300
    col: 0
    width: 24
    height: 6

  - name: ADH_OFFENDER_SUPERVISION
    title: ADH_OFFENDER_SUPERVISION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_OFFENDER_SUPERVISION.primary_key,
      us_mi_ADH_OFFENDER_SUPERVISION.offender_supervision_id,
      us_mi_ADH_OFFENDER_SUPERVISION.offender_booking_id,
      us_mi_ADH_OFFENDER_SUPERVISION.supervision_level_id,
      us_mi_ADH_OFFENDER_SUPERVISION.description,
      us_mi_ADH_OFFENDER_SUPERVISION.effective_date__raw,
      us_mi_ADH_OFFENDER_SUPERVISION.expiration_date__raw,
      us_mi_ADH_OFFENDER_SUPERVISION.active_flag,
      us_mi_ADH_OFFENDER_SUPERVISION.next_review_date,
      us_mi_ADH_OFFENDER_SUPERVISION.begin_date,
      us_mi_ADH_OFFENDER_SUPERVISION.jail_treatment_release_date,
      us_mi_ADH_OFFENDER_SUPERVISION.last_update_user,
      us_mi_ADH_OFFENDER_SUPERVISION.last_update_date__raw,
      us_mi_ADH_OFFENDER_SUPERVISION.last_update_node,
      us_mi_ADH_OFFENDER_SUPERVISION.mdoc_paid_phone_rpt,
      us_mi_ADH_OFFENDER_SUPERVISION.file_id,
      us_mi_ADH_OFFENDER_SUPERVISION.is_deleted]
    sorts: [us_mi_ADH_OFFENDER_SUPERVISION.effective_date__raw]
    note_display: hover
    note_text: "This table contains information about a person on supervision within MIDOC."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 306
    col: 0
    width: 24
    height: 6

  - name: ADH_PERSONAL_PROTECTION_ORDER_NOTE
    title: ADH_PERSONAL_PROTECTION_ORDER_NOTE
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.primary_key,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.notes_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.offender_booking_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.sequence_number,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.notes,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.last_update_user,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.last_update_date__raw,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.last_update_node,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.file_id,
      us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.is_deleted]
    sorts: [us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.last_update_date__raw]
    note_display: hover
    note_text: "This table contains information on notes about personal protection orders issued against people under MIDOC jurisdiction."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 312
    col: 0
    width: 24
    height: 6

  - name: ADH_PLAN_OF_SUPERVISION
    title: ADH_PLAN_OF_SUPERVISION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_PLAN_OF_SUPERVISION.primary_key,
      us_mi_ADH_PLAN_OF_SUPERVISION.plan_of_supervision_id,
      us_mi_ADH_PLAN_OF_SUPERVISION.offender_booking_id,
      us_mi_ADH_PLAN_OF_SUPERVISION.supervision_type_id,
      us_mi_ADH_PLAN_OF_SUPERVISION.start_date__raw,
      us_mi_ADH_PLAN_OF_SUPERVISION.end_date__raw,
      us_mi_ADH_PLAN_OF_SUPERVISION.notes,
      us_mi_ADH_PLAN_OF_SUPERVISION.last_update_user,
      us_mi_ADH_PLAN_OF_SUPERVISION.last_update_date__raw,
      us_mi_ADH_PLAN_OF_SUPERVISION.last_update_node,
      us_mi_ADH_PLAN_OF_SUPERVISION.file_id,
      us_mi_ADH_PLAN_OF_SUPERVISION.is_deleted]
    sorts: [us_mi_ADH_PLAN_OF_SUPERVISION.start_date__raw]
    note_display: hover
    note_text: "OMNI-database table for plans of supervision"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 318
    col: 0
    width: 24
    height: 6

  - name: ADH_SUBSTANCE_ABUSE_TEST
    title: ADH_SUBSTANCE_ABUSE_TEST
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_SUBSTANCE_ABUSE_TEST.primary_key,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.substance_abuse_test_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.offender_booking_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.specimen_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.sample_taken_flag,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.sample_not_taken_reason_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.sample_type_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.test_taker_type_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.test_taker_employee_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.test_taker_other_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.test_schedule_print_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.lab_sample_ref_code,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.reason_tested_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.overall_result_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.sample_void_reason_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.patch_length_of_wear_days,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.completion_of_wear_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.patch_removal_reason_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.testing_lab_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.lab_test_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.misconduct_incident_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.offender_type_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.notes,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.test_result_create_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.positive_response_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.positive_response_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.specimen_location_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.last_update_user,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.last_update_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.last_update_node,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.record_source_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.program_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.record_sent_to_lab_flag,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.record_sent_to_lab_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.default_drug_panel_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.print_flag,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.print_date__raw,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.printed_by_user,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.file_id,
      us_mi_ADH_SUBSTANCE_ABUSE_TEST.is_deleted]
    sorts: [us_mi_ADH_SUBSTANCE_ABUSE_TEST.specimen_date__raw]
    note_display: hover
    note_text: "OMNI-database table for substance abuse test records"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 324
    col: 0
    width: 24
    height: 6

  - name: ADH_SUPERVISION_CONDITION
    title: ADH_SUPERVISION_CONDITION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_SUPERVISION_CONDITION.primary_key,
      us_mi_ADH_SUPERVISION_CONDITION.supervision_condition_id,
      us_mi_ADH_SUPERVISION_CONDITION.special_condition_id,
      us_mi_ADH_SUPERVISION_CONDITION.corporation_id,
      us_mi_ADH_SUPERVISION_CONDITION.person_id,
      us_mi_ADH_SUPERVISION_CONDITION.legal_order_id,
      us_mi_ADH_SUPERVISION_CONDITION.offender_booking_id,
      us_mi_ADH_SUPERVISION_CONDITION.payment_amount,
      us_mi_ADH_SUPERVISION_CONDITION.frequency,
      us_mi_ADH_SUPERVISION_CONDITION.due_date,
      us_mi_ADH_SUPERVISION_CONDITION.total_amount,
      us_mi_ADH_SUPERVISION_CONDITION.status_id,
      us_mi_ADH_SUPERVISION_CONDITION.active_flag,
      us_mi_ADH_SUPERVISION_CONDITION.recorded_date__raw,
      us_mi_ADH_SUPERVISION_CONDITION.compliance_flag,
      us_mi_ADH_SUPERVISION_CONDITION.set_date__raw,
      us_mi_ADH_SUPERVISION_CONDITION.bail_flag,
      us_mi_ADH_SUPERVISION_CONDITION.original_condition_flag,
      us_mi_ADH_SUPERVISION_CONDITION.notes,
      us_mi_ADH_SUPERVISION_CONDITION.notes2,
      us_mi_ADH_SUPERVISION_CONDITION.notes3,
      us_mi_ADH_SUPERVISION_CONDITION.add_disposition_date,
      us_mi_ADH_SUPERVISION_CONDITION.add_authority_id,
      us_mi_ADH_SUPERVISION_CONDITION.add_disposition_notes,
      us_mi_ADH_SUPERVISION_CONDITION.close_request_date,
      us_mi_ADH_SUPERVISION_CONDITION.close_request_employee_id,
      us_mi_ADH_SUPERVISION_CONDITION.close_request_notes,
      us_mi_ADH_SUPERVISION_CONDITION.closing_reason_code,
      us_mi_ADH_SUPERVISION_CONDITION.closing_authority_id,
      us_mi_ADH_SUPERVISION_CONDITION.closing_date__raw,
      us_mi_ADH_SUPERVISION_CONDITION.closing_notes,
      us_mi_ADH_SUPERVISION_CONDITION.create_reason_code_id,
      us_mi_ADH_SUPERVISION_CONDITION.last_update_user,
      us_mi_ADH_SUPERVISION_CONDITION.last_update_date,
      us_mi_ADH_SUPERVISION_CONDITION.last_update_node,
      us_mi_ADH_SUPERVISION_CONDITION.file_id,
      us_mi_ADH_SUPERVISION_CONDITION.is_deleted]
    sorts: [us_mi_ADH_SUPERVISION_CONDITION.recorded_date__raw]
    note_display: hover
    note_text: "This table contains information about a person's supervision conditions."
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 330
    col: 0
    width: 24
    height: 6

  - name: ADH_SUPERVISION_VIOLATION
    title: ADH_SUPERVISION_VIOLATION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_SUPERVISION_VIOLATION.primary_key,
      us_mi_ADH_SUPERVISION_VIOLATION.supervision_violation_id,
      us_mi_ADH_SUPERVISION_VIOLATION.offender_booking_id,
      us_mi_ADH_SUPERVISION_VIOLATION.legal_order_id,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_recommendation_id,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_status_id,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_score,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_assessment_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_assessment_result_id,
      us_mi_ADH_SUPERVISION_VIOLATION.available_flag,
      us_mi_ADH_SUPERVISION_VIOLATION.available_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.due_process_max_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.waived_preliminary_flag,
      us_mi_ADH_SUPERVISION_VIOLATION.prelim_hearing_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.prelim_hearing_location_id,
      us_mi_ADH_SUPERVISION_VIOLATION.prelim_hearing_employee_id,
      us_mi_ADH_SUPERVISION_VIOLATION.preliminary_decision_id,
      us_mi_ADH_SUPERVISION_VIOLATION.formal_hearing_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.formal_hearing_location_id,
      us_mi_ADH_SUPERVISION_VIOLATION.formal_hearing_employee_id,
      us_mi_ADH_SUPERVISION_VIOLATION.formal_hearing_disp_id,
      us_mi_ADH_SUPERVISION_VIOLATION.arraignment_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.arraignment_location_id,
      us_mi_ADH_SUPERVISION_VIOLATION.arraignment_judge,
      us_mi_ADH_SUPERVISION_VIOLATION.active_flag,
      us_mi_ADH_SUPERVISION_VIOLATION.date_violation_charges_served__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.court_room_number,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_type_id,
      us_mi_ADH_SUPERVISION_VIOLATION.violation_invest_begin_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.notes,
      us_mi_ADH_SUPERVISION_VIOLATION.parolee_comments_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.procedural_matters_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.plea_agreement_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.conclusion_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.ale_recommendation_type_id,
      us_mi_ADH_SUPERVISION_VIOLATION.ale_recommendation_term_id,
      us_mi_ADH_SUPERVISION_VIOLATION.summary_of_evidence_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.findings_of_fact_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.mitigation_summary_notes_id,
      us_mi_ADH_SUPERVISION_VIOLATION.foa_representative_id,
      us_mi_ADH_SUPERVISION_VIOLATION.foa_recommendation_id,
      us_mi_ADH_SUPERVISION_VIOLATION.offender_representative,
      us_mi_ADH_SUPERVISION_VIOLATION.offender_representative_id,
      us_mi_ADH_SUPERVISION_VIOLATION.defense_recommendation_id,
      us_mi_ADH_SUPERVISION_VIOLATION.formal_hearing_arraign_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.formal_hearing_status_id,
      us_mi_ADH_SUPERVISION_VIOLATION.video_hearing_flag,
      us_mi_ADH_SUPERVISION_VIOLATION.last_update_user,
      us_mi_ADH_SUPERVISION_VIOLATION.last_update_date__raw,
      us_mi_ADH_SUPERVISION_VIOLATION.last_update_node,
      us_mi_ADH_SUPERVISION_VIOLATION.file_id,
      us_mi_ADH_SUPERVISION_VIOLATION.is_deleted]
    sorts: [us_mi_ADH_SUPERVISION_VIOLATION.violation_assessment_date__raw]
    note_display: hover
    note_text: "OMNI-database table recording supervision violations"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 336
    col: 0
    width: 24
    height: 6

  - name: ADH_SUPER_COND_VIOLATION
    title: ADH_SUPER_COND_VIOLATION
    explore: us_mi_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mi_ADH_SUPER_COND_VIOLATION.primary_key,
      us_mi_ADH_SUPER_COND_VIOLATION.super_cond_violation_id,
      us_mi_ADH_SUPER_COND_VIOLATION.supervision_condition_id,
      us_mi_ADH_SUPER_COND_VIOLATION.supv_violation_incident_id,
      us_mi_ADH_SUPER_COND_VIOLATION.supervision_violation_id,
      us_mi_ADH_SUPER_COND_VIOLATION.offender_booking_id,
      us_mi_ADH_SUPER_COND_VIOLATION.prelim_hearing_finding_id,
      us_mi_ADH_SUPER_COND_VIOLATION.prelim_hearing_finding_note_id,
      us_mi_ADH_SUPER_COND_VIOLATION.prelim_hearing_finding_notes,
      us_mi_ADH_SUPER_COND_VIOLATION.formal_hearing_finding_id,
      us_mi_ADH_SUPER_COND_VIOLATION.formal_hearing_finding_notes,
      us_mi_ADH_SUPER_COND_VIOLATION.notes_id,
      us_mi_ADH_SUPER_COND_VIOLATION.notes,
      us_mi_ADH_SUPER_COND_VIOLATION.notes2,
      us_mi_ADH_SUPER_COND_VIOLATION.notes3,
      us_mi_ADH_SUPER_COND_VIOLATION.last_update_user,
      us_mi_ADH_SUPER_COND_VIOLATION.last_update_date__raw,
      us_mi_ADH_SUPER_COND_VIOLATION.last_update_node,
      us_mi_ADH_SUPER_COND_VIOLATION.file_id,
      us_mi_ADH_SUPER_COND_VIOLATION.is_deleted]
    sorts: [us_mi_ADH_SUPER_COND_VIOLATION.last_update_date__raw]
    note_display: hover
    note_text: "OMNI-database table linking supervision violations, supervision conditions, and supervision incidents"
    listen: 
      View Type: us_mi_ADH_OFFENDER.view_type
      US_MI_DOC_ID: us_mi_ADH_OFFENDER.offender_id
      US_MI_DOC: us_mi_ADH_OFFENDER.offender_number
      US_MI_DOC_BOOK: us_mi_ADH_OFFENDER_BOOKING.offender_booking_id
    row: 342
    col: 0
    width: 24
    height: 6

