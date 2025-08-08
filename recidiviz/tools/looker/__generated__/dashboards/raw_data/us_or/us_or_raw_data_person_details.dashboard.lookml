# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_or_raw_data_person_details
  title: Oregon Raw Data Person Details
  description: For examining individuals in US_OR's raw data tables
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
    model: "@{project_id}"
    explore: us_or_raw_data
    field: us_or_RCDVZ_PRDDTA_OP970P.view_type

  - name: US_OR_RECORD_KEY
    title: US_OR_RECORD_KEY
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_or_raw_data
    field: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY

  elements:
  - name: RCDVZ_PRDDTA_OP970P
    title: RCDVZ_PRDDTA_OP970P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP970P.primary_key,
      us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP970P.DATE_LAST_UPDATED,
      us_or_RCDVZ_PRDDTA_OP970P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.TRANSFER_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.CURRENT_STATUS,
      us_or_RCDVZ_PRDDTA_OP970P.RESPONSIBLE_LOCATION,
      us_or_RCDVZ_PRDDTA_OP970P.ID_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.RESPONSIBLE_DIVISION,
      us_or_RCDVZ_PRDDTA_OP970P.FIRST_NAME,
      us_or_RCDVZ_PRDDTA_OP970P.LAST_NAME,
      us_or_RCDVZ_PRDDTA_OP970P.MIDDLE_NAME,
      us_or_RCDVZ_PRDDTA_OP970P.BIRTHDATE,
      us_or_RCDVZ_PRDDTA_OP970P.CELL_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.ADMISSION_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.ADMISSION_REASON,
      us_or_RCDVZ_PRDDTA_OP970P.OUTCOUNT_REASON,
      us_or_RCDVZ_PRDDTA_OP970P.OUTCOUNT_LOCATION,
      us_or_RCDVZ_PRDDTA_OP970P.ORS_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.ORS_PARAGRAPH,
      us_or_RCDVZ_PRDDTA_OP970P.CRIME_ABBREVIATION,
      us_or_RCDVZ_PRDDTA_OP970P.CRIME_CLASS,
      us_or_RCDVZ_PRDDTA_OP970P.OFF_SEVERITY,
      us_or_RCDVZ_PRDDTA_OP970P.CRIME_CATEGORY,
      us_or_RCDVZ_PRDDTA_OP970P.CASELOAD,
      us_or_RCDVZ_PRDDTA_OP970P.INSTITUTION_RISK,
      us_or_RCDVZ_PRDDTA_OP970P.COMMUNITY_SUPER_LVL,
      us_or_RCDVZ_PRDDTA_OP970P.HISTORY_RISK,
      us_or_RCDVZ_PRDDTA_OP970P.DANG_OFFENDER,
      us_or_RCDVZ_PRDDTA_OP970P.SXDANG_OFFENDER,
      us_or_RCDVZ_PRDDTA_OP970P.PROJECTED_RELEASE_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.GT_PROJ_RELE_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.ET_PROJ_RELE_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.CURRENT_CUSTODY_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.PAROLE_RELEASE_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.MAXIMUM_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.SENT_GUIDELINES_APP,
      us_or_RCDVZ_PRDDTA_OP970P.MAX_INCAR_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.SG_EARNED_TIME_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.PVP_FLAG,
      us_or_RCDVZ_PRDDTA_OP970P.PVP_CYCLE,
      us_or_RCDVZ_PRDDTA_OP970P.FLAG_137635,
      us_or_RCDVZ_PRDDTA_OP970P.NEXT_EXPI_DATE,
      us_or_RCDVZ_PRDDTA_OP970P.LIFE_OR_DEATH,
      us_or_RCDVZ_PRDDTA_OP970P.PROGRAM_ASSIGNMENT_NAME,
      us_or_RCDVZ_PRDDTA_OP970P.PRGSTRDT,
      us_or_RCDVZ_PRDDTA_OP970P.SEX,
      us_or_RCDVZ_PRDDTA_OP970P.RACE,
      us_or_RCDVZ_PRDDTA_OP970P.HEIGHT,
      us_or_RCDVZ_PRDDTA_OP970P.WEIGHT,
      us_or_RCDVZ_PRDDTA_OP970P.HAIR,
      us_or_RCDVZ_PRDDTA_OP970P.EYES,
      us_or_RCDVZ_PRDDTA_OP970P.SMOKER,
      us_or_RCDVZ_PRDDTA_OP970P.SEXUAL_PREFERENCE,
      us_or_RCDVZ_PRDDTA_OP970P.MARITAL_STATUS,
      us_or_RCDVZ_PRDDTA_OP970P.LOCATION_CODE,
      us_or_RCDVZ_PRDDTA_OP970P.UNIT_NUMBER,
      us_or_RCDVZ_PRDDTA_OP970P.CUSTODY_STATUS,
      us_or_RCDVZ_PRDDTA_OP970P.FILLER,
      us_or_RCDVZ_PRDDTA_OP970P.DEPENDENTS,
      us_or_RCDVZ_PRDDTA_OP970P.RELIGIOUS_PREFERENCE,
      us_or_RCDVZ_PRDDTA_OP970P.EMPLOYMENT_STATUS,
      us_or_RCDVZ_PRDDTA_OP970P.PRIMARY_OCCUPATION,
      us_or_RCDVZ_PRDDTA_OP970P.GANG_CODE,
      us_or_RCDVZ_PRDDTA_OP970P.file_id,
      us_or_RCDVZ_PRDDTA_OP970P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY]
    note_display: hover
    note_text: "Extensive rollup of all Adult in Custody (AIC) information."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 0
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CLOVER
    title: RCDVZ_CISPRDDTA_CLOVER
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CLOVER.primary_key,
      us_or_RCDVZ_CISPRDDTA_CLOVER.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CLOVER.EFFECTIVE_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CLOVER.CLASS_ACTION_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CLOVER.CLASS_USER,
      us_or_RCDVZ_CISPRDDTA_CLOVER.SEQUENCE_NO,
      us_or_RCDVZ_CISPRDDTA_CLOVER.INSTITUTION_RISK,
      us_or_RCDVZ_CISPRDDTA_CLOVER.COMMENT76,
      us_or_RCDVZ_CISPRDDTA_CLOVER.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CLOVER.file_id,
      us_or_RCDVZ_CISPRDDTA_CLOVER.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CLOVER.EFFECTIVE_DATE__raw]
    note_display: hover
    note_text: "This is the classification override table - only in this table if overridden."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 6
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CMCROH
    title: RCDVZ_CISPRDDTA_CMCROH
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CMCROH.primary_key,
      us_or_RCDVZ_CISPRDDTA_CMCROH.CASELOAD,
      us_or_RCDVZ_CISPRDDTA_CMCROH.SIGNIFICANT_CONTACT_YES_NO,
      us_or_RCDVZ_CISPRDDTA_CMCROH.MONTHLY_REPORT_YES_NO,
      us_or_RCDVZ_CISPRDDTA_CMCROH.NEXT_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMCROH.CHRONO_WHAT,
      us_or_RCDVZ_CISPRDDTA_CMCROH.CHRONO_WHO,
      us_or_RCDVZ_CISPRDDTA_CMCROH.CHRONO_TYPE,
      us_or_RCDVZ_CISPRDDTA_CMCROH.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CMCROH.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_CMCROH.ENTRY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMCROH.CHRONO_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMCROH.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CMCROH.file_id,
      us_or_RCDVZ_CISPRDDTA_CMCROH.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CMCROH.LAST_UPDATED_WHEN__raw]
    note_display: hover
    note_text: "This file contains information about contact standards."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 12
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CMOFFT
    title: RCDVZ_CISPRDDTA_CMOFFT
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CMOFFT.primary_key,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.SUBFILE_KEY,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.TREAT_ID,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.REFER_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.ENTRY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.EXIT_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.EXIT_CODE,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.CUSTODY_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.ADMISSION_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.INDIGENT_Y_N,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.file_id,
      us_or_RCDVZ_CISPRDDTA_CMOFFT.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CMOFFT.REFER_DATE__raw]
    note_display: hover
    note_text: "This table contains treatment program information for Adults on Supervision."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 18
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CMOFRH
    title: RCDVZ_CISPRDDTA_CMOFRH
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CMOFRH.primary_key,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.ASSESSMENT_TYPE,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.RISK_ASSESSMENT_TOTAL,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.CALCULATED_SUPER_LVL,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.COMMUNITY_SUPER_LVL,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.CUSTODY_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.ADMISSION_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.CASELOAD,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.RESPONSIBLE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.CURRENT_STATUS,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.LAST_UPDATE_PROGRAM,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.RECORD_ADD_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.RECORD_ADD_PROGRAM,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.ENTRY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.ASSESSMENT_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.ASSESSMENT_RULE_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.RECORD_ADD_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.file_id,
      us_or_RCDVZ_CISPRDDTA_CMOFRH.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CMOFRH.ENTRY_DATE__raw]
    note_display: hover
    note_text: "This file contains information about community supervision levels and assesments taken."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 24
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CMSACN
    title: RCDVZ_CISPRDDTA_CMSACN
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CMSACN.primary_key,
      us_or_RCDVZ_CISPRDDTA_CMSACN.SANC_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSACN.INCIDENT_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSACN.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CMSACN.SANC_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSACN.CONDITION_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSACN.SEQUENCE_NO,
      us_or_RCDVZ_CISPRDDTA_CMSACN.CONTESTED,
      us_or_RCDVZ_CISPRDDTA_CMSACN.SUPPORTED,
      us_or_RCDVZ_CISPRDDTA_CMSACN.file_id,
      us_or_RCDVZ_CISPRDDTA_CMSACN.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CMSACN.SANC_DATE__raw]
    note_display: hover
    note_text: "This file contains Sactioned Condition information."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 30
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CMSACO
    title: RCDVZ_CISPRDDTA_CMSACO
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CMSACO.primary_key,
      us_or_RCDVZ_CISPRDDTA_CMSACO.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CMSACO.SANC_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSACO.COURT_CASE_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSACO.COUNTY,
      us_or_RCDVZ_CISPRDDTA_CMSACO.SEQUENCE_NO,
      us_or_RCDVZ_CISPRDDTA_CMSACO.SANCTION_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSACO.REC_CUST_UNITS,
      us_or_RCDVZ_CISPRDDTA_CMSACO.CMS_APPLNID,
      us_or_RCDVZ_CISPRDDTA_CMSACO.SANCTION_AUTHORIZATION_TABLE,
      us_or_RCDVZ_CISPRDDTA_CMSACO.REC_AUTH_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSACO.GIVEN_SANC_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSACO.GIVEN_CUST_UNITS,
      us_or_RCDVZ_CISPRDDTA_CMSACO.GIVEN_AUTH_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSACO.JUDGE_SANCTION,
      us_or_RCDVZ_CISPRDDTA_CMSACO.JUDGE_UNITS,
      us_or_RCDVZ_CISPRDDTA_CMSACO.MORRISEY_APPLNID,
      us_or_RCDVZ_CISPRDDTA_CMSACO.OFFENDER_BEHAVIOR_TABLE,
      us_or_RCDVZ_CISPRDDTA_CMSACO.OFFENDER_BEHAVIOR,
      us_or_RCDVZ_CISPRDDTA_CMSACO.MAX_CUST_UNITS,
      us_or_RCDVZ_CISPRDDTA_CMSACO.EXCEPTION_SANC_IMPOSED,
      us_or_RCDVZ_CISPRDDTA_CMSACO.file_id,
      us_or_RCDVZ_CISPRDDTA_CMSACO.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CMSACO.RECORD_KEY, us_or_RCDVZ_CISPRDDTA_CMSACO.SANC_NUMBER, us_or_RCDVZ_CISPRDDTA_CMSACO.SEQUENCE_NO]
    note_display: hover
    note_text: "This file contains information about Sanctions Imposed by Court Case."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 36
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_CMSAIM
    title: RCDVZ_CISPRDDTA_CMSAIM
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_CMSAIM.primary_key,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.CUSTODY_BEGIN_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.CUSTODY_END_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SANC_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.NOTIFY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.HEARING_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.DECISION_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SANC_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SANCTION_TYPE,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.CUSTODY_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.ADMISSION_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.CMS_APPLNID,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SANCTION_ACTION_TABLE,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SANCTION_ACTION,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.COMMUNITY_SUPER_LVL,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.HIST_RISK_RECLASS_SCORE,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.LOCATION_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.CASELOAD,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.CONDITION_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SANCTION_CODE,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.COURT_CASE_NUMBER,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.COUNTY,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.LOCAL_SANCTION_FLAG,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.EXTRADITION,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.ADVICE_OF_RIGHTS,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.HEARING_WAIVER_FLAG,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.SUPPORTING_DOCUMENTATION,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.WAIVE_10_DAY_APPEAL,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.BAF_NO4,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.file_id,
      us_or_RCDVZ_CISPRDDTA_CMSAIM.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_CMSAIM.CUSTODY_BEGIN_DATE__raw]
    note_display: hover
    note_text: "This file contains information on Imposed Sanctions."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 42
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_MTOFDR
    title: RCDVZ_CISPRDDTA_MTOFDR
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_MTOFDR.primary_key,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.NEXT_NUMBER,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.CASE_DATE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.MTA_APPLNID,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LOCATION_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.CASE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.CASE_SEQUENCE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.HEARING_LOCATION,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.HEARING_OFFICER_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.HEARING_OFFICER,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DECISION_STATUS,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DECISION_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DECISION_HO_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DECISION_FINAL_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.EFFECTIVE_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.NOTES,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DR_COMMENT,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LANGUAGE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.CIS_APPLNID,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LANGUAGE_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.CELL_NUMBER,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.PROG_ID,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LOCATION_CODE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.MISCONDUCT_LOCATION,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.CASE_TIME,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DISPOSITION_OF_EVIDENCE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.IMMEDIATE_ACTION,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.USER_TITLE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.SUBMITTED_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.REVIEW_SUPERVISOR,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.SUPERVISOR_TITLE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DATE_OF_REVIEW__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DELIVERY_USER,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DELIVERY_TITLE,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DELIVERY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.DOCKET_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LAST_UPDATE_PROGRAM,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.file_id,
      us_or_RCDVZ_CISPRDDTA_MTOFDR.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_MTOFDR.DECISION_DATE__raw]
    note_display: hover
    note_text: "Disciplinary Report. Summary of offender misconducts/discipinary reports."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 48
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_MTRULE
    title: RCDVZ_CISPRDDTA_MTRULE
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_MTRULE.primary_key,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_MTRULE.NEXT_NUMBER,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_ALLEGED,
      us_or_RCDVZ_CISPRDDTA_MTRULE.SEQUENCE_NO,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_FOUND,
      us_or_RCDVZ_CISPRDDTA_MTRULE.MTA_APPLNID,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_FINDING_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_FINDING,
      us_or_RCDVZ_CISPRDDTA_MTRULE.LEVEL_STATUS_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTRULE.INCENT_LEVEL_STATUS,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_PLEA,
      us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_PLEA_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTRULE.MISCONDUCT_CHARGE_ID,
      us_or_RCDVZ_CISPRDDTA_MTRULE.file_id,
      us_or_RCDVZ_CISPRDDTA_MTRULE.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_MTRULE.RECORD_KEY, us_or_RCDVZ_CISPRDDTA_MTRULE.NEXT_NUMBER, us_or_RCDVZ_CISPRDDTA_MTRULE.RULE_ALLEGED, us_or_RCDVZ_CISPRDDTA_MTRULE.SEQUENCE_NO]
    note_display: hover
    note_text: "TODO(#17148): Fill in with answer from OR"
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 54
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_MTSANC
    title: RCDVZ_CISPRDDTA_MTSANC
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_MTSANC.primary_key,
      us_or_RCDVZ_CISPRDDTA_MTSANC.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_MTSANC.NEXT_NUMBER,
      us_or_RCDVZ_CISPRDDTA_MTSANC.RULE_ALLEGED,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SEQUENCE_NO,
      us_or_RCDVZ_CISPRDDTA_MTSANC.DISCIPLINARY_SANCTION,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SUBFILE_KEY,
      us_or_RCDVZ_CISPRDDTA_MTSANC.START_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTSANC.STOP_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTSANC.ACTUAL_DAYS,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SANCTION_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SUSPENSE_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SANCTION_ACTUAL_END_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTSANC.POSTED_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_MTSANC.MTA_APPLNID,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SANCTION_TABLE,
      us_or_RCDVZ_CISPRDDTA_MTSANC.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_MTSANC.MISCONDUCT_SANCTION_ID,
      us_or_RCDVZ_CISPRDDTA_MTSANC.SANCTION_ADJUST_ID,
      us_or_RCDVZ_CISPRDDTA_MTSANC.file_id,
      us_or_RCDVZ_CISPRDDTA_MTSANC.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_MTSANC.START_DATE__raw]
    note_display: hover
    note_text: "Disciplinary Sanction. Sanction resulting from AIC's misconduct."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 60
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_OPCOND
    title: RCDVZ_CISPRDDTA_OPCOND
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_OPCOND.primary_key,
      us_or_RCDVZ_CISPRDDTA_OPCOND.CONDITION_TYPE,
      us_or_RCDVZ_CISPRDDTA_OPCOND.CUSTODY_NUMBER,
      us_or_RCDVZ_CISPRDDTA_OPCOND.ADMISSION_NUMBER,
      us_or_RCDVZ_CISPRDDTA_OPCOND.BAF_NO,
      us_or_RCDVZ_CISPRDDTA_OPCOND.CONDITION_CODE,
      us_or_RCDVZ_CISPRDDTA_OPCOND.CONDITION_TEXT,
      us_or_RCDVZ_CISPRDDTA_OPCOND.COND_AMOUNT_TYPE,
      us_or_RCDVZ_CISPRDDTA_OPCOND.CONDITION_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_OPCOND.MONTHLY_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_OPCOND.PAID_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_OPCOND.CONDITION_TRACKABLE,
      us_or_RCDVZ_CISPRDDTA_OPCOND.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_OPCOND.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_OPCOND.COURT_CASE_NUMBER,
      us_or_RCDVZ_CISPRDDTA_OPCOND.COUNTY,
      us_or_RCDVZ_CISPRDDTA_OPCOND.COND_TERMINATION_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCOND.LAST_ACTIVITY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCOND.PAYMENT_START_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCOND.COND_SAT_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCOND.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_OPCOND.file_id,
      us_or_RCDVZ_CISPRDDTA_OPCOND.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_OPCOND.COND_TERMINATION_DATE__raw]
    note_display: hover
    note_text: "This file contains inforamtion about conditions."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 66
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDTA_OPCONE
    title: RCDVZ_CISPRDDTA_OPCONE
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDTA_OPCONE.primary_key,
      us_or_RCDVZ_CISPRDDTA_OPCONE.RECORD_KEY,
      us_or_RCDVZ_CISPRDDTA_OPCONE.SEQUENCE_NO,
      us_or_RCDVZ_CISPRDDTA_OPCONE.COURT_CASE_NUMBER,
      us_or_RCDVZ_CISPRDDTA_OPCONE.COUNTY,
      us_or_RCDVZ_CISPRDDTA_OPCONE.CONDITION_TYPE,
      us_or_RCDVZ_CISPRDDTA_OPCONE.CUSTODY_NUMBER,
      us_or_RCDVZ_CISPRDDTA_OPCONE.ADMISSION_NUMBER,
      us_or_RCDVZ_CISPRDDTA_OPCONE.BAF_NO,
      us_or_RCDVZ_CISPRDDTA_OPCONE.CONDITION_CODE,
      us_or_RCDVZ_CISPRDDTA_OPCONE.CONDITION_TEXT,
      us_or_RCDVZ_CISPRDDTA_OPCONE.COND_TERMINATION_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCONE.COND_AMOUNT_TYPE,
      us_or_RCDVZ_CISPRDDTA_OPCONE.CONDITION_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_OPCONE.MONTHLY_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_OPCONE.PAID_AMOUNT,
      us_or_RCDVZ_CISPRDDTA_OPCONE.LAST_ACTIVITY_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCONE.PAYMENT_START_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCONE.COND_SAT_DATE__raw,
      us_or_RCDVZ_CISPRDDTA_OPCONE.CONDITION_TRACKABLE,
      us_or_RCDVZ_CISPRDDTA_OPCONE.LAST_UPDATE_LOCATION,
      us_or_RCDVZ_CISPRDDTA_OPCONE.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_CISPRDDTA_OPCONE.file_id,
      us_or_RCDVZ_CISPRDDTA_OPCONE.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDTA_OPCONE.COND_TERMINATION_DATE__raw]
    note_display: hover
    note_text: "This file contains information on expired conditions."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 72
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_CISPRDDT_CLCLHD
    title: RCDVZ_CISPRDDT_CLCLHD
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_CISPRDDT_CLCLHD.primary_key,
      us_or_RCDVZ_CISPRDDT_CLCLHD.RECORD_KEY,
      us_or_RCDVZ_CISPRDDT_CLCLHD.EFFECTIVE_DATE__raw,
      us_or_RCDVZ_CISPRDDT_CLCLHD.CLASS_ACTION_DATE__raw,
      us_or_RCDVZ_CISPRDDT_CLCLHD.PUBLIC_SCORE,
      us_or_RCDVZ_CISPRDDT_CLCLHD.INST_SCORE,
      us_or_RCDVZ_CISPRDDT_CLCLHD.INSTITUTION_RISK,
      us_or_RCDVZ_CISPRDDT_CLCLHD.CLASS_STATUS,
      us_or_RCDVZ_CISPRDDT_CLCLHD.CLS_APPLNID,
      us_or_RCDVZ_CISPRDDT_CLCLHD.CLASS_STATUS_TABLE,
      us_or_RCDVZ_CISPRDDT_CLCLHD.COUNSELOR_APPROVAL,
      us_or_RCDVZ_CISPRDDT_CLCLHD.CHAIR_APPROVAL,
      us_or_RCDVZ_CISPRDDT_CLCLHD.ADMIN_APPROVAL,
      us_or_RCDVZ_CISPRDDT_CLCLHD.CASELOAD,
      us_or_RCDVZ_CISPRDDT_CLCLHD.PRIOR_CRIMES,
      us_or_RCDVZ_CISPRDDT_CLCLHD.file_id,
      us_or_RCDVZ_CISPRDDT_CLCLHD.is_deleted]
    sorts: [us_or_RCDVZ_CISPRDDT_CLCLHD.EFFECTIVE_DATE__raw]
    note_display: hover
    note_text: "This file contains Adult in Custody (AIC) Assessment Header information."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 78
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP007P
    title: RCDVZ_PRDDTA_OP007P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP007P.primary_key,
      us_or_RCDVZ_PRDDTA_OP007P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP007P.SEX,
      us_or_RCDVZ_PRDDTA_OP007P.RACE,
      us_or_RCDVZ_PRDDTA_OP007P.HEIGHT,
      us_or_RCDVZ_PRDDTA_OP007P.WEIGHT,
      us_or_RCDVZ_PRDDTA_OP007P.HAIR,
      us_or_RCDVZ_PRDDTA_OP007P.EYES,
      us_or_RCDVZ_PRDDTA_OP007P.HANDICAP,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_SIGHT,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_HEARING,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_SPEECH,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_MOVEMENT,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_INTELL,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_EMOTIONAL,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_OTHER,
      us_or_RCDVZ_PRDDTA_OP007P.IMPAIRMENT_COMMENT,
      us_or_RCDVZ_PRDDTA_OP007P.CITIZEN,
      us_or_RCDVZ_PRDDTA_OP007P.BIRTHPLACE,
      us_or_RCDVZ_PRDDTA_OP007P.SMOKER,
      us_or_RCDVZ_PRDDTA_OP007P.SEXUAL_PREFERENCE,
      us_or_RCDVZ_PRDDTA_OP007P.MARITAL_STATUS,
      us_or_RCDVZ_PRDDTA_OP007P.DEPENDENTS,
      us_or_RCDVZ_PRDDTA_OP007P.EMPLOYMENT_STATUS,
      us_or_RCDVZ_PRDDTA_OP007P.PRIMARY_OCCUPATION,
      us_or_RCDVZ_PRDDTA_OP007P.MILITARY_BRANCH,
      us_or_RCDVZ_PRDDTA_OP007P.MILITARY_RELEASE_DATE,
      us_or_RCDVZ_PRDDTA_OP007P.MILITARY_RELEASE_TYPE,
      us_or_RCDVZ_PRDDTA_OP007P.MILITARY_RELEASE_TYPE2,
      us_or_RCDVZ_PRDDTA_OP007P.GANG_CODE,
      us_or_RCDVZ_PRDDTA_OP007P.ASSAULTIVE_BEHAVIOR,
      us_or_RCDVZ_PRDDTA_OP007P.MEDICAL_CODE,
      us_or_RCDVZ_PRDDTA_OP007P.JOB_CODE,
      us_or_RCDVZ_PRDDTA_OP007P.COUNTY,
      us_or_RCDVZ_PRDDTA_OP007P.END_TRAIN_DATE,
      us_or_RCDVZ_PRDDTA_OP007P.file_id,
      us_or_RCDVZ_PRDDTA_OP007P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP007P.RECORD_KEY]
    note_display: hover
    note_text: "This file contains basic information about the Adults in Custody (AIC) such as sex, race, height, etc.."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 84
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP008P
    title: RCDVZ_PRDDTA_OP008P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP008P.primary_key,
      us_or_RCDVZ_PRDDTA_OP008P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP008P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP008P.CUSTODY_DATE,
      us_or_RCDVZ_PRDDTA_OP008P.CUSTODY_TYPE,
      us_or_RCDVZ_PRDDTA_OP008P.CUSTODY_FROM,
      us_or_RCDVZ_PRDDTA_OP008P.DISCHARGE_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP008P.DISCHARGE_TYPE,
      us_or_RCDVZ_PRDDTA_OP008P.file_id,
      us_or_RCDVZ_PRDDTA_OP008P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP008P.DISCHARGE_DATE__raw]
    note_display: hover
    note_text: "Custody and Discharge. AIC custody cycle information. Includes information about the period of time that the department had jurisdiction over an AIC."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 90
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP009P
    title: RCDVZ_PRDDTA_OP009P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP009P.primary_key,
      us_or_RCDVZ_PRDDTA_OP009P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP009P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP009P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP009P.ADMISSION_LOCATION,
      us_or_RCDVZ_PRDDTA_OP009P.ADMISSION_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP009P.PREVIOUS_STATUS,
      us_or_RCDVZ_PRDDTA_OP009P.ADMISSION_FROM_LOCATION,
      us_or_RCDVZ_PRDDTA_OP009P.CURRENT_STATUS,
      us_or_RCDVZ_PRDDTA_OP009P.NEW_CRIME_DATE,
      us_or_RCDVZ_PRDDTA_OP009P.LEAVE_SCHEDULED_DATE,
      us_or_RCDVZ_PRDDTA_OP009P.RELEASE_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP009P.RELEASE_REASON,
      us_or_RCDVZ_PRDDTA_OP009P.VIOLATION_TYPE,
      us_or_RCDVZ_PRDDTA_OP009P.RELEASE_TO_LOCATION,
      us_or_RCDVZ_PRDDTA_OP009P.file_id,
      us_or_RCDVZ_PRDDTA_OP009P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP009P.ADMISSION_DATE__raw]
    note_display: hover
    note_text: "Admission and Release. Adult in Custody (AIC_ admission and release cycle information. Includes information on the  period and type of supervision given an AIC. Indicates reason for release from a supervision status."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 96
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP010P
    title: RCDVZ_PRDDTA_OP010P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP010P.primary_key,
      us_or_RCDVZ_PRDDTA_OP010P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP010P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP010P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_NUMBER,
      us_or_RCDVZ_PRDDTA_OP010P.RESPONSIBLE_DIVISION,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_IN_LOCATION,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_IN_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_REASON,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_FROM_LOCATION,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_TO_LOCATION,
      us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_TO_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP010P.file_id,
      us_or_RCDVZ_PRDDTA_OP010P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP010P.TRANSFER_IN_DATE__raw]
    note_display: hover
    note_text: "Transfer. Adult in Custody (AIC) transfer cycle information. Includes information on the period and location of  supervision of an AIC.  Indicates the reason an AIC was moved from a given location."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 102
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP011P
    title: RCDVZ_PRDDTA_OP011P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP011P.primary_key,
      us_or_RCDVZ_PRDDTA_OP011P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP011P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP011P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP011P.TRANSFER_NUMBER,
      us_or_RCDVZ_PRDDTA_OP011P.FACILITY,
      us_or_RCDVZ_PRDDTA_OP011P.CELL_NUMBER,
      us_or_RCDVZ_PRDDTA_OP011P.OUTCOUNT_REASON,
      us_or_RCDVZ_PRDDTA_OP011P.OUTCOUNT_LOCATION,
      us_or_RCDVZ_PRDDTA_OP011P.MOVE_IN_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP011P.PREV_FACILITY,
      us_or_RCDVZ_PRDDTA_OP011P.PREV_CELL_NUMBER,
      us_or_RCDVZ_PRDDTA_OP011P.PREV_OUTCOUNT_REAS,
      us_or_RCDVZ_PRDDTA_OP011P.PREV_OUTCOUNT_LOCA,
      us_or_RCDVZ_PRDDTA_OP011P.MOVE_OUT_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP011P.ENTRY_DATE,
      us_or_RCDVZ_PRDDTA_OP011P.LAST_UPDATED_WHEN,
      us_or_RCDVZ_PRDDTA_OP011P.CASELOAD,
      us_or_RCDVZ_PRDDTA_OP011P.file_id,
      us_or_RCDVZ_PRDDTA_OP011P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP011P.MOVE_IN_DATE__raw]
    note_display: hover
    note_text: "Housing. Adult in Custody (AIC) housing information. Includes information on the period an AIC spent in a cell, on an  outcount, or on a caseload.  More accurate information on caseload history can be found in table CMCOFH."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 108
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP013P
    title: RCDVZ_PRDDTA_OP013P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP013P.primary_key,
      us_or_RCDVZ_PRDDTA_OP013P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP013P.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_PRDDTA_OP013P.UPDATE_JOB_NAME,
      us_or_RCDVZ_PRDDTA_OP013P.UPDATE_USER_ID,
      us_or_RCDVZ_PRDDTA_OP013P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP013P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP013P.TRANSFER_NUMBER,
      us_or_RCDVZ_PRDDTA_OP013P.CURRENT_STATUS,
      us_or_RCDVZ_PRDDTA_OP013P.RESPONSIBLE_LOCATION,
      us_or_RCDVZ_PRDDTA_OP013P.ID_NUMBER,
      us_or_RCDVZ_PRDDTA_OP013P.RESPONSIBLE_DIVISION,
      us_or_RCDVZ_PRDDTA_OP013P.SUPERVISING_AUTHORITY,
      us_or_RCDVZ_PRDDTA_OP013P.FIRST_NAME,
      us_or_RCDVZ_PRDDTA_OP013P.LAST_NAME,
      us_or_RCDVZ_PRDDTA_OP013P.MIDDLE_NAME,
      us_or_RCDVZ_PRDDTA_OP013P.NAME_KEY,
      us_or_RCDVZ_PRDDTA_OP013P.BIRTHDATE,
      us_or_RCDVZ_PRDDTA_OP013P.CELL_NUMBER,
      us_or_RCDVZ_PRDDTA_OP013P.ADMISSION_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP013P.ADMISSION_REASON,
      us_or_RCDVZ_PRDDTA_OP013P.OUTCOUNT_REASON,
      us_or_RCDVZ_PRDDTA_OP013P.OUTCOUNT_LOCATION,
      us_or_RCDVZ_PRDDTA_OP013P.BAF_NO,
      us_or_RCDVZ_PRDDTA_OP013P.BAF_GROUP_NO,
      us_or_RCDVZ_PRDDTA_OP013P.ORS_NUMBER,
      us_or_RCDVZ_PRDDTA_OP013P.ORS_PARAGRAPH,
      us_or_RCDVZ_PRDDTA_OP013P.CRIME_ABBREVIATION,
      us_or_RCDVZ_PRDDTA_OP013P.CRIME_CLASS,
      us_or_RCDVZ_PRDDTA_OP013P.OFF_SEVERITY,
      us_or_RCDVZ_PRDDTA_OP013P.CRIME_CATEGORY,
      us_or_RCDVZ_PRDDTA_OP013P.CASELOAD,
      us_or_RCDVZ_PRDDTA_OP013P.INSTITUTION_RISK,
      us_or_RCDVZ_PRDDTA_OP013P.COMMUNITY_SUPER_LVL,
      us_or_RCDVZ_PRDDTA_OP013P.HISTORY_RISK,
      us_or_RCDVZ_PRDDTA_OP013P.DANG_OFFENDER,
      us_or_RCDVZ_PRDDTA_OP013P.SXDANG_OFFENDER,
      us_or_RCDVZ_PRDDTA_OP013P.PROJECTED_RELEASE_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.EXTRA_GOOD_TIME,
      us_or_RCDVZ_PRDDTA_OP013P.GT_PROJ_RELE_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.ET_PROJ_RELE_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.CURRENT_CUSTODY_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.PAROLE_RELEASE_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.MAXIMUM_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.PROGRAM_ASSIGNMENT_NAME,
      us_or_RCDVZ_PRDDTA_OP013P.SEX_ASSESSMENT,
      us_or_RCDVZ_PRDDTA_OP013P.ASSESSMENT_SCORE,
      us_or_RCDVZ_PRDDTA_OP013P.MAX_INCAR_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.ASSESSMENT_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.MINIMUM_DATE,
      us_or_RCDVZ_PRDDTA_OP013P.LIFE_OR_DEATH,
      us_or_RCDVZ_PRDDTA_OP013P.file_id,
      us_or_RCDVZ_PRDDTA_OP013P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP013P.LAST_UPDATED_WHEN__raw]
    note_display: hover
    note_text: "This file contains a rollup of current information for an AIC."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 114
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP053P
    title: RCDVZ_PRDDTA_OP053P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP053P.primary_key,
      us_or_RCDVZ_PRDDTA_OP053P.NEXT_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP053P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.OFFENSE_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_PRDDTA_OP053P.ARREST_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP053P.COUNTY,
      us_or_RCDVZ_PRDDTA_OP053P.COURT_CASE_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.COURT_CASE_COUNT,
      us_or_RCDVZ_PRDDTA_OP053P.CRIME_COMMITTED_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP053P.DISTRICT_ATTORNEY,
      us_or_RCDVZ_PRDDTA_OP053P.DEFENSE_COUNSEL,
      us_or_RCDVZ_PRDDTA_OP053P.JUDGE,
      us_or_RCDVZ_PRDDTA_OP053P.CONVICTED_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP053P.DA_CASE_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.ORS_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.ORS_PARAGRAPH,
      us_or_RCDVZ_PRDDTA_OP053P.INCHOATE,
      us_or_RCDVZ_PRDDTA_OP053P.INCHOATE_ORS_NUMBER,
      us_or_RCDVZ_PRDDTA_OP053P.INCHOATE_ORS_PARAGRAPH,
      us_or_RCDVZ_PRDDTA_OP053P.FELONY_IS_MISDEMEANOR,
      us_or_RCDVZ_PRDDTA_OP053P.CRIME_ABBREVIATION,
      us_or_RCDVZ_PRDDTA_OP053P.CRIME_CLASS,
      us_or_RCDVZ_PRDDTA_OP053P.CRIME_CATEGORY,
      us_or_RCDVZ_PRDDTA_OP053P.OFF_SEVERITY,
      us_or_RCDVZ_PRDDTA_OP053P.OOS_FED_CRIME_IND,
      us_or_RCDVZ_PRDDTA_OP053P.MATRIX_RANGE_FROM,
      us_or_RCDVZ_PRDDTA_OP053P.MATRIX_RANGE_TO,
      us_or_RCDVZ_PRDDTA_OP053P.BAF_NO,
      us_or_RCDVZ_PRDDTA_OP053P.BAF_GROUP_NO,
      us_or_RCDVZ_PRDDTA_OP053P.BOARD_OVRRIDE,
      us_or_RCDVZ_PRDDTA_OP053P.PSI_PSR_NO,
      us_or_RCDVZ_PRDDTA_OP053P.SENT_GUIDELINES_APP,
      us_or_RCDVZ_PRDDTA_OP053P.file_id,
      us_or_RCDVZ_PRDDTA_OP053P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP053P.LAST_UPDATED_WHEN__raw]
    note_display: hover
    note_text: "Offense Description. History of AIC offenses. Includes court case number, county and count; ORS number, description  and categories; arrest, crime committed and convicted dates; and DA and DA case number, defense counsel and judge."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 120
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OP054P
    title: RCDVZ_PRDDTA_OP054P
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OP054P.primary_key,
      us_or_RCDVZ_PRDDTA_OP054P.NEXT_NUMBER,
      us_or_RCDVZ_PRDDTA_OP054P.CHARGE_NEXT_NUMBER,
      us_or_RCDVZ_PRDDTA_OP054P.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OP054P.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OP054P.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OP054P.OFFENSE_NUMBER,
      us_or_RCDVZ_PRDDTA_OP054P.SENTENCE_NUMBER,
      us_or_RCDVZ_PRDDTA_OP054P.LAST_UPDATED_WHEN__raw,
      us_or_RCDVZ_PRDDTA_OP054P.SENTENCE_TYPE,
      us_or_RCDVZ_PRDDTA_OP054P.TENT_PARO_DISC_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.SENTENCE_BEGIN_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP054P.TOLLING_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP054P.COMPACT_BEGIN_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.SENTENCE_LENGTH_YEARS,
      us_or_RCDVZ_PRDDTA_OP054P.SENTENCE_LENGTH_MONTHS,
      us_or_RCDVZ_PRDDTA_OP054P.SENTENCE_LENGTH_DAYS,
      us_or_RCDVZ_PRDDTA_OP054P.LIFE_OR_DEATH,
      us_or_RCDVZ_PRDDTA_OP054P.CONSEC_TO,
      us_or_RCDVZ_PRDDTA_OP054P.MERGE_SENTENCE,
      us_or_RCDVZ_PRDDTA_OP054P.TIME_SERVED,
      us_or_RCDVZ_PRDDTA_OP054P.TS_ELIGIBLE_ET,
      us_or_RCDVZ_PRDDTA_OP054P.ET_RETRACTED_TS,
      us_or_RCDVZ_PRDDTA_OP054P.INOPERATIVE_DAYS,
      us_or_RCDVZ_PRDDTA_OP054P.GOOD_TIME_WITHHELD,
      us_or_RCDVZ_PRDDTA_OP054P.GOODTIME_DAYS_RESTORED,
      us_or_RCDVZ_PRDDTA_OP054P.EXTRA_GOODTIME,
      us_or_RCDVZ_PRDDTA_OP054P.MINIMUM_161,
      us_or_RCDVZ_PRDDTA_OP054P.GUNMIN_161610_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.MINIMUM_144,
      us_or_RCDVZ_PRDDTA_OP054P.MINIMUM_163105,
      us_or_RCDVZ_PRDDTA_OP054P.MURMIN_163105_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.MINIMUM_163115,
      us_or_RCDVZ_PRDDTA_OP054P.MURMIN_163115_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.FLAG_137635,
      us_or_RCDVZ_PRDDTA_OP054P.DANG_OFFENDER,
      us_or_RCDVZ_PRDDTA_OP054P.DANG_OFFENDER_SENT,
      us_or_RCDVZ_PRDDTA_OP054P.PRESUMPTIVE_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.PARO_REVO_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.PV_REMAINING_SENTENCE,
      us_or_RCDVZ_PRDDTA_OP054P.PROJECTED_EXPIRAT_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.STATUTORY_GOOD_TIME,
      us_or_RCDVZ_PRDDTA_OP054P.APPLY_EARNED_TIME,
      us_or_RCDVZ_PRDDTA_OP054P.EARNED_TIME_DAYS,
      us_or_RCDVZ_PRDDTA_OP054P.APPLY_PARTIAL_ET,
      us_or_RCDVZ_PRDDTA_OP054P.M40_EARNED_TIME,
      us_or_RCDVZ_PRDDTA_OP054P.APPLICATION_ID,
      us_or_RCDVZ_PRDDTA_OP054P.TABLE_ID,
      us_or_RCDVZ_PRDDTA_OP054P.M40_STATUS,
      us_or_RCDVZ_PRDDTA_OP054P.SG_EARNED_TIME_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.SG_20_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.PROJECTED_COMPLETION_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.MAXIMUM_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP054P.MINIMUM_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP054P.MANUAL_SENTCALC_USERID,
      us_or_RCDVZ_PRDDTA_OP054P.MANUAL_SENTCALC_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.PAROLE_RELEASE_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.M11_SENTENCE,
      us_or_RCDVZ_PRDDTA_OP054P.PAROLE_DENIED,
      us_or_RCDVZ_PRDDTA_OP054P.BOARDER_COMPACT_FLAG,
      us_or_RCDVZ_PRDDTA_OP054P.MERIT_GT_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.TERMINATION_CODE,
      us_or_RCDVZ_PRDDTA_OP054P.TERMINATION_DATE__raw,
      us_or_RCDVZ_PRDDTA_OP054P.PV_CODE,
      us_or_RCDVZ_PRDDTA_OP054P.INOP_DAYS_AFTER_REVOKE,
      us_or_RCDVZ_PRDDTA_OP054P.PPS_SENTENCE_YEARS,
      us_or_RCDVZ_PRDDTA_OP054P.PPS_SENTENCE_MONTHS,
      us_or_RCDVZ_PRDDTA_OP054P.PPS_SENTENCE_DAYS,
      us_or_RCDVZ_PRDDTA_OP054P.PPS_BEGIN_DATE,
      us_or_RCDVZ_PRDDTA_OP054P.SENT_ADD_LOCA,
      us_or_RCDVZ_PRDDTA_OP054P.FUTURE_HEARING_PENDING,
      us_or_RCDVZ_PRDDTA_OP054P.SECOND_LOOK_GROUP,
      us_or_RCDVZ_PRDDTA_OP054P.PB_DEFERAL_DAYS,
      us_or_RCDVZ_PRDDTA_OP054P.file_id,
      us_or_RCDVZ_PRDDTA_OP054P.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OP054P.LAST_UPDATED_WHEN__raw]
    note_display: hover
    note_text: "Sentence Calculation. History of AIC sentences. Includes sentence type, begin date, sentence length, time served,  sentence consecutive to or merged with, minimums, good time, earned time, maximum termination date, actual  termination date and code, and other related flags."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 126
    col: 0
    width: 24
    height: 6

  - name: RCDVZ_PRDDTA_OPCOUR
    title: RCDVZ_PRDDTA_OPCOUR
    explore: us_or_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_or_RCDVZ_PRDDTA_OPCOUR.primary_key,
      us_or_RCDVZ_PRDDTA_OPCOUR.RECORD_KEY,
      us_or_RCDVZ_PRDDTA_OPCOUR.COURT_CASE_NUMBER,
      us_or_RCDVZ_PRDDTA_OPCOUR.COUNTY,
      us_or_RCDVZ_PRDDTA_OPCOUR.STRUCTURED_SANCTION_STATUS,
      us_or_RCDVZ_PRDDTA_OPCOUR.SG_HISTORY_SCALE,
      us_or_RCDVZ_PRDDTA_OPCOUR.CURRENT_STATUS,
      us_or_RCDVZ_PRDDTA_OPCOUR.SENTENCE_GUIDELINES_APP,
      us_or_RCDVZ_PRDDTA_OPCOUR.CUSTODY_NUMBER,
      us_or_RCDVZ_PRDDTA_OPCOUR.ADMISSION_NUMBER,
      us_or_RCDVZ_PRDDTA_OPCOUR.JUDGE,
      us_or_RCDVZ_PRDDTA_OPCOUR.DISTRICT_ATTORNEY,
      us_or_RCDVZ_PRDDTA_OPCOUR.DA_CASE_NUMBER,
      us_or_RCDVZ_PRDDTA_OPCOUR.DEFENSE_COUNSEL,
      us_or_RCDVZ_PRDDTA_OPCOUR.TERMINATION_CODE,
      us_or_RCDVZ_PRDDTA_OPCOUR.TERMINATION_DATE__raw,
      us_or_RCDVZ_PRDDTA_OPCOUR.SANCTION_SUPV_CODE,
      us_or_RCDVZ_PRDDTA_OPCOUR.STRUCTURED_SANC_SUPV_DATE,
      us_or_RCDVZ_PRDDTA_OPCOUR.SENTENCE_BEGIN_DATE__raw,
      us_or_RCDVZ_PRDDTA_OPCOUR.CRIME_CATEGORY,
      us_or_RCDVZ_PRDDTA_OPCOUR.file_id,
      us_or_RCDVZ_PRDDTA_OPCOUR.is_deleted]
    sorts: [us_or_RCDVZ_PRDDTA_OPCOUR.TERMINATION_DATE__raw]
    note_display: hover
    note_text: "Court Case. History of AIC court cases. Contains information that is duplicated from tables OP053P and OP054P as well as some additional fields. Additional information includes Sentencing Guidelines criminal history level, court case termination reason and date, and sanction supervision status."
    listen: 
      View Type: us_or_RCDVZ_PRDDTA_OP970P.view_type
      US_OR_RECORD_KEY: us_or_RCDVZ_PRDDTA_OP970P.RECORD_KEY
    row: 132
    col: 0
    width: 24
    height: 6

