# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_az_raw_data_person_details_template
  title: Arizona Latest Raw Data Person Details Template
  description: For examining individuals in US_AZ's raw data tables
  layout: newspaper
  load_configuration: wait
  extension: required

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
    explore: us_az_raw_data
    field: us_az_PERSON.view_type

  - name: US_AZ_PERSON_ID
    title: US_AZ_PERSON_ID
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    explore: us_az_raw_data
    field: us_az_PERSON.PERSON_ID

  elements:
  - name: PERSON
    title: PERSON
    explore: us_az_raw_data
    type: looker_grid
    fields: [us_az_PERSON.primary_key,
      us_az_PERSON.PERSON_ID,
      us_az_PERSON.PERSON_TYPE_ID,
      us_az_PERSON.FIRST_NAME,
      us_az_PERSON.MIDDLE_NAME,
      us_az_PERSON.SURNAME,
      us_az_PERSON.DATE_OF_BIRTH__raw,
      us_az_PERSON.GENDER,
      us_az_PERSON.TITLE,
      us_az_PERSON.SUFFIX,
      us_az_PERSON.CHANGE_ID,
      us_az_PERSON.NO_NAME_PROVIDED,
      us_az_PERSON.RESTRICTED_OWNER,
      us_az_PERSON.DOB_UNKNOWN,
      us_az_PERSON.PERSON_TYPE_OTHER,
      us_az_PERSON.DOC_FACILITY_ID,
      us_az_PERSON.HOUSING_STATUS_ID,
      us_az_PERSON.DATE_STATUS_CHANGED,
      us_az_PERSON.OFFICE_LOCATION_ID,
      us_az_PERSON.MAIDEN_NAME,
      us_az_PERSON.IS_MERGED,
      us_az_PERSON.STATUS_ID,
      us_az_PERSON.ADC_NUMBER,
      us_az_PERSON.CREATE_USERID,
      us_az_PERSON.CREATE_DTM__raw,
      us_az_PERSON.UPDT_USERID,
      us_az_PERSON.UPDT_DTM__raw,
      us_az_PERSON.DOC_UNIT_ID,
      us_az_PERSON.DONOT_REHIRE_FLAG,
      us_az_PERSON.file_id,
      us_az_PERSON.is_deleted]
    sorts: [us_az_PERSON.DATE_OF_BIRTH__raw]
    note_display: hover
    note_text: "A master table of people involved with ADCRR. Includes residents, parole and probation clients, staff members, visitors to ADCRR facilities, and possibly some judges."
    listen: 
      View Type: us_az_PERSON.view_type
      US_AZ_PERSON_ID: us_az_PERSON.PERSON_ID
    row: 0
    col: 0
    width: 24
    height: 6

  - name: DEMOGRAPHICS
    title: DEMOGRAPHICS
    explore: us_az_raw_data
    type: looker_grid
    fields: [us_az_DEMOGRAPHICS.primary_key,
      us_az_DEMOGRAPHICS.DEMOGRAPHIC_ID,
      us_az_DEMOGRAPHICS.PERSON_ID,
      us_az_DEMOGRAPHICS.RACE,
      us_az_DEMOGRAPHICS.SKINTONE,
      us_az_DEMOGRAPHICS.HEIGHT_FT,
      us_az_DEMOGRAPHICS.HEIGHT_INCH,
      us_az_DEMOGRAPHICS.WEIGHT_LBS,
      us_az_DEMOGRAPHICS.EYE_COLOUR,
      us_az_DEMOGRAPHICS.HAIR_COLOUR,
      us_az_DEMOGRAPHICS.HAIR_STYLE,
      us_az_DEMOGRAPHICS.HAIR_LENGTH,
      us_az_DEMOGRAPHICS.FACIAL_HAIR,
      us_az_DEMOGRAPHICS.COUNTRY_OF_CITIZENSHIP,
      us_az_DEMOGRAPHICS.STATE,
      us_az_DEMOGRAPHICS.CHANGE_ID,
      us_az_DEMOGRAPHICS.JUVENILE_WAIVED,
      us_az_DEMOGRAPHICS.DOC_ID,
      us_az_DEMOGRAPHICS.PBMS_ID,
      us_az_DEMOGRAPHICS.MARITAL_STATUS_ID,
      us_az_DEMOGRAPHICS.GERIATRIC_WAIVED,
      us_az_DEMOGRAPHICS.PREA_STATUS_ID,
      us_az_DEMOGRAPHICS.ORIG_FACILITY_ID,
      us_az_DEMOGRAPHICS.TRACKING_NUMBER,
      us_az_DEMOGRAPHICS.CASE_NUMBER,
      us_az_DEMOGRAPHICS.FBI_NUMBER,
      us_az_DEMOGRAPHICS.BOOKING_NUMBER,
      us_az_DEMOGRAPHICS.DPP_ID,
      us_az_DEMOGRAPHICS.PRIMARY_LANGUAGE,
      us_az_DEMOGRAPHICS.INTERPRETER_REQUIRED,
      us_az_DEMOGRAPHICS.NO_DEPENDANT_CHILDREN,
      us_az_DEMOGRAPHICS.RELIGION,
      us_az_DEMOGRAPHICS.ETHNICITY,
      us_az_DEMOGRAPHICS.YRS_IN_DOC_STATE,
      us_az_DEMOGRAPHICS.MTHS_IN_DOC_STATE,
      us_az_DEMOGRAPHICS.SHOE_SIZE,
      us_az_DEMOGRAPHICS.PLACE_OF_BIRTH,
      us_az_DEMOGRAPHICS.STATE_OF_BIRTH_ID,
      us_az_DEMOGRAPHICS.COUNTRY_OF_BIRTH_ID,
      us_az_DEMOGRAPHICS.PHYSICAL_BUILD_ID,
      us_az_DEMOGRAPHICS.MOTHER_MAIDEN_FIRST_NAME,
      us_az_DEMOGRAPHICS.MOTHER_MAIDEN_SURNAME,
      us_az_DEMOGRAPHICS.CREATE_USERID,
      us_az_DEMOGRAPHICS.CREATE_DTM__raw,
      us_az_DEMOGRAPHICS.UPDT_USERID,
      us_az_DEMOGRAPHICS.UPDT_DTM__raw,
      us_az_DEMOGRAPHICS.ACTIVE_FLAG,
      us_az_DEMOGRAPHICS.file_id,
      us_az_DEMOGRAPHICS.is_deleted]
    sorts: [us_az_DEMOGRAPHICS.CREATE_DTM__raw]
    note_display: hover
    note_text: "Demographic information, indexed on person, for people found in the PERSON table."
    listen: 
      View Type: us_az_PERSON.view_type
      US_AZ_PERSON_ID: us_az_PERSON.PERSON_ID
    row: 6
    col: 0
    width: 24
    height: 6

  - name: OCCUPANCY
    title: OCCUPANCY
    explore: us_az_raw_data
    type: looker_grid
    fields: [us_az_OCCUPANCY.primary_key,
      us_az_OCCUPANCY.OCCUPANCY_TYPE_ID,
      us_az_OCCUPANCY.LOCATION_ID,
      us_az_OCCUPANCY.OCCUPANCY_ID,
      us_az_OCCUPANCY.PERSON_ID,
      us_az_OCCUPANCY.DATE_FROM__raw,
      us_az_OCCUPANCY.DATE_TO__raw,
      us_az_OCCUPANCY.DETAILS,
      us_az_OCCUPANCY.CURRENT_OCCUPANCY,
      us_az_OCCUPANCY.CHANGE_ID,
      us_az_OCCUPANCY.NO_INFO_PROVIDED,
      us_az_OCCUPANCY.HOMELESS,
      us_az_OCCUPANCY.DOC_ID,
      us_az_OCCUPANCY.DPP_ID,
      us_az_OCCUPANCY.COMMENT_HOMELESS,
      us_az_OCCUPANCY.CREATE_USERID,
      us_az_OCCUPANCY.CREATE_DTM__raw,
      us_az_OCCUPANCY.UPDT_USERID,
      us_az_OCCUPANCY.UPDT_DTM__raw,
      us_az_OCCUPANCY.file_id,
      us_az_OCCUPANCY.is_deleted]
    sorts: [us_az_OCCUPANCY.DATE_FROM__raw]
    note_display: hover
    note_text: "Personal occupancy history; including permanent addresses, cities in which a person is/was homeless, and DOC facilities. All locations are listed as strings of numbers, which can be decoded by using LOCATION_ID as a foreign key with the LOCATION table."
    listen: 
      View Type: us_az_PERSON.view_type
      US_AZ_PERSON_ID: us_az_PERSON.PERSON_ID
    row: 12
    col: 0
    width: 24
    height: 6

  - name: DPP_EPISODE
    title: DPP_EPISODE
    explore: us_az_raw_data
    type: looker_grid
    fields: [us_az_DPP_EPISODE.primary_key,
      us_az_DPP_EPISODE.DPP_ID,
      us_az_DPP_EPISODE.PERSON_ID,
      us_az_DPP_EPISODE.DPP_NUMBER,
      us_az_DPP_EPISODE.CASE_TYPE,
      us_az_DPP_EPISODE.INTERVIEW_DATE__raw,
      us_az_DPP_EPISODE.REVIEWER_ID,
      us_az_DPP_EPISODE.STATUS_ID,
      us_az_DPP_EPISODE.CHANGE_ID,
      us_az_DPP_EPISODE.TRACKING_NUMBER,
      us_az_DPP_EPISODE.INTERVIEW_STATUS_ID,
      us_az_DPP_EPISODE.DATE_COMPLETED__raw,
      us_az_DPP_EPISODE.DATE_RECEIVED__raw,
      us_az_DPP_EPISODE.ECC_ELIGIBILITY_STATUS,
      us_az_DPP_EPISODE.ECC_ABATEMENT_DATE__raw,
      us_az_DPP_EPISODE.ECC_ABATEMENT_DATE_DENIED,
      us_az_DPP_EPISODE.COURT_TITLE,
      us_az_DPP_EPISODE.COURT_FIRST_NAME,
      us_az_DPP_EPISODE.COURT_MIDDLE_NAME,
      us_az_DPP_EPISODE.COURT_LAST_NAME,
      us_az_DPP_EPISODE.COURT_SUFFIX,
      us_az_DPP_EPISODE.COURT_DATE_OF_BIRTH__raw,
      us_az_DPP_EPISODE.SUPERVISION_LEVEL_ID,
      us_az_DPP_EPISODE.SUPERVISION_LEVEL_STARTDATE__raw,
      us_az_DPP_EPISODE.SUPERVISION_LEVEL_ENDDATE__raw,
      us_az_DPP_EPISODE.RELEASE_TYPE_ID,
      us_az_DPP_EPISODE.CREATE_USERID,
      us_az_DPP_EPISODE.CREATE_DTM__raw,
      us_az_DPP_EPISODE.UPDT_DTM__raw,
      us_az_DPP_EPISODE.UPDT_USERID,
      us_az_DPP_EPISODE.ACTIVE_FLAG,
      us_az_DPP_EPISODE.LAST_CONTACT_DATE__raw,
      us_az_DPP_EPISODE.file_id,
      us_az_DPP_EPISODE.is_deleted]
    sorts: [us_az_DPP_EPISODE.INTERVIEW_DATE__raw]
    note_display: hover
    note_text: "This table contains information that was true at the time of intake for each stint a person spends on supervision. Some fields are updated once a person is released to liberty. This  table can be used to deduce the active supervision population, and serves as a base table for our supervision periods ingest view query and ADCRR's 960 report."
    listen: 
      View Type: us_az_PERSON.view_type
      US_AZ_PERSON_ID: us_az_PERSON.PERSON_ID
    row: 18
    col: 0
    width: 24
    height: 6

  - name: DOC_EPISODE
    title: DOC_EPISODE
    explore: us_az_raw_data
    type: looker_grid
    fields: [us_az_DOC_EPISODE.primary_key,
      us_az_DOC_EPISODE.DOC_ID,
      us_az_DOC_EPISODE.PERSON_ID,
      us_az_DOC_EPISODE.DOC_NUMBER,
      us_az_DOC_EPISODE.BOOKING_NUMBER,
      us_az_DOC_EPISODE.CHANGE_ID,
      us_az_DOC_EPISODE.INTERVIEW_STATUS_ID,
      us_az_DOC_EPISODE.SENTENCE_STATUS_ID,
      us_az_DOC_EPISODE.REASON_CONFINEMENT_ID,
      us_az_DOC_EPISODE.DO_NOT_DISCLOSE,
      us_az_DOC_EPISODE.HEADQUARTERS,
      us_az_DOC_EPISODE.EN_ROUTE,
      us_az_DOC_EPISODE.DOC_FACILITY_ID,
      us_az_DOC_EPISODE.RESTRICTIVE_STATUS_ID,
      us_az_DOC_EPISODE.RESTRICTIVE_STATUS_DATE,
      us_az_DOC_EPISODE.PREV_RESTRIC_STATUS_ID,
      us_az_DOC_EPISODE.MPC_COUNTY_ID,
      us_az_DOC_EPISODE.JAIL_LOCATION_ID,
      us_az_DOC_EPISODE.PIA_LOCATION_ID,
      us_az_DOC_EPISODE.PIA_STATE_ID,
      us_az_DOC_EPISODE.ICC_TO_ID,
      us_az_DOC_EPISODE.SECURITY_LEVEL_ID,
      us_az_DOC_EPISODE.SECURITY_LEVEL_DATE__raw,
      us_az_DOC_EPISODE.CURRENT_STATUS_DATE__raw,
      us_az_DOC_EPISODE.IS_NO_TIME_CREDIT_RECORD,
      us_az_DOC_EPISODE.RETURN_TYPE_ID,
      us_az_DOC_EPISODE.DEMOGRAPHIC_COUNTY_ID,
      us_az_DOC_EPISODE.DEMOGRAPHIC_STATE_ID,
      us_az_DOC_EPISODE.OLD_REASON_CONFINEMENT_DESC,
      us_az_DOC_EPISODE.CR_END_DATE__raw,
      us_az_DOC_EPISODE.COURT_TITLE,
      us_az_DOC_EPISODE.COURT_FIRST_NAME,
      us_az_DOC_EPISODE.COURT_MIDDLE_NAME,
      us_az_DOC_EPISODE.COURT_LAST_NAME,
      us_az_DOC_EPISODE.COURT_SUFFIX,
      us_az_DOC_EPISODE.COURT_DATE_OF_BIRTH__raw,
      us_az_DOC_EPISODE.LAST_DATE_UPDATED__raw,
      us_az_DOC_EPISODE.ADMISSION_DATE__raw,
      us_az_DOC_EPISODE.INTAKE_COMPLETE_FLAG,
      us_az_DOC_EPISODE.ARS_NUMBER,
      us_az_DOC_EPISODE.CR_NUMBER,
      us_az_DOC_EPISODE.CREATE_USERID,
      us_az_DOC_EPISODE.CREATE_DTM__raw,
      us_az_DOC_EPISODE.UPDT_USERID,
      us_az_DOC_EPISODE.UPDT_DTM__raw,
      us_az_DOC_EPISODE.WORK_LEVEL_ID,
      us_az_DOC_EPISODE.DPP_ID,
      us_az_DOC_EPISODE.ADMISSION_TYPE,
      us_az_DOC_EPISODE.file_id,
      us_az_DOC_EPISODE.is_deleted]
    sorts: [us_az_DOC_EPISODE.SECURITY_LEVEL_DATE__raw]
    note_display: hover
    note_text: "This table contains information about periods of incarceration. Fields are updated in place over time as conditions of an incarceration stint change."
    listen: 
      View Type: us_az_PERSON.view_type
      US_AZ_PERSON_ID: us_az_PERSON.PERSON_ID
    row: 24
    col: 0
    width: 24
    height: 6

  - name: LOOKUPS
    title: LOOKUPS
    explore: us_az_raw_data
    type: looker_grid
    fields: [us_az_LOOKUPS.primary_key,
      us_az_LOOKUPS.LOOKUP_ID,
      us_az_LOOKUPS.LOOKUP_CATEGORY,
      us_az_LOOKUPS.DESCRIPTION,
      us_az_LOOKUPS.CODE,
      us_az_LOOKUPS.OTHER,
      us_az_LOOKUPS.ACTIVE,
      us_az_LOOKUPS.PRIORITY,
      us_az_LOOKUPS.OTHER_2,
      us_az_LOOKUPS.OTHER_3,
      us_az_LOOKUPS.LOCALE_EN,
      us_az_LOOKUPS.LOCALE_DE,
      us_az_LOOKUPS.DESCRIPTION_DE,
      us_az_LOOKUPS.LOCALE_FR,
      us_az_LOOKUPS.DESCRIPTION_FR,
      us_az_LOOKUPS.LOCALE_NL,
      us_az_LOOKUPS.DESCRIPTION_NL,
      us_az_LOOKUPS.OTHER_4,
      us_az_LOOKUPS.PARENT_LOOKUP_ID,
      us_az_LOOKUPS.LOOK_CREATE_USERID,
      us_az_LOOKUPS.LOOK_CREATE_DTM__raw,
      us_az_LOOKUPS.LOOK_UPDT_USERID,
      us_az_LOOKUPS.LOOK_UPDT_DTM__raw,
      us_az_LOOKUPS.file_id,
      us_az_LOOKUPS.is_deleted]
    sorts: [us_az_LOOKUPS.LOOK_CREATE_DTM__raw]
    note_display: hover
    note_text: "A master lookup table for all values that may need to be looked up. Includes demographic descriptors, types of people involved in the system, types of facilities, types of detainers, classifications, and many more."
    listen: 
      View Type: us_az_PERSON.view_type
      US_AZ_PERSON_ID: us_az_PERSON.PERSON_ID
    row: 30
    col: 0
    width: 24
    height: 6

