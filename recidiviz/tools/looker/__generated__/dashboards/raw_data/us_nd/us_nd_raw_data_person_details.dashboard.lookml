# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_nd_raw_data_person_details
  title: North Dakota Raw Data Person Details
  description: For examining individuals in US_ND's raw data tables
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
    explore: us_nd_raw_data
    field: us_nd_docstars_offenders.view_type

  - name: US_ND_SID
    title: US_ND_SID
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_nd_raw_data
    field: us_nd_docstars_offenders.SID

  - name: US_ND_ELITE_BOOKING
    title: US_ND_ELITE_BOOKING
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_nd_raw_data
    field: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID

  - name: US_ND_ELITE
    title: US_ND_ELITE
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_nd_raw_data
    field: us_nd_elite_offenders.ROOT_OFFENDER_ID

  elements:
  - name: docstars_offenders
    title: docstars_offenders
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_docstars_offenders.primary_key,
      us_nd_docstars_offenders.SID,
      us_nd_docstars_offenders.ITAGROOT_ID,
      us_nd_docstars_offenders.LAST_NAME,
      us_nd_docstars_offenders.FIRST,
      us_nd_docstars_offenders.MIDDLE,
      us_nd_docstars_offenders.ADDRESS,
      us_nd_docstars_offenders.CITY,
      us_nd_docstars_offenders.STATE,
      us_nd_docstars_offenders.ZIP,
      us_nd_docstars_offenders.DOB__raw,
      us_nd_docstars_offenders.AGENT,
      us_nd_docstars_offenders.SUP_LVL,
      us_nd_docstars_offenders.SUPER_OVERRIDE,
      us_nd_docstars_offenders.PREVIOUS_AGENT,
      us_nd_docstars_offenders.RECORD_STATUS,
      us_nd_docstars_offenders.COMPLETION_IND,
      us_nd_docstars_offenders.ALIASFLAG,
      us_nd_docstars_offenders.ADDRESS2,
      us_nd_docstars_offenders.CITY2,
      us_nd_docstars_offenders.STATE2,
      us_nd_docstars_offenders.ZIP2,
      us_nd_docstars_offenders.SITEID,
      us_nd_docstars_offenders.ABSCONDER,
      us_nd_docstars_offenders.SEXOFF,
      us_nd_docstars_offenders.GOODTIMEDATE__raw,
      us_nd_docstars_offenders.RACE,
      us_nd_docstars_offenders.SEX,
      us_nd_docstars_offenders.C_MARITAL,
      us_nd_docstars_offenders.D_DEP,
      us_nd_docstars_offenders.E_LIV_ARR,
      us_nd_docstars_offenders.F_VETERAN,
      us_nd_docstars_offenders.G_INCOME,
      us_nd_docstars_offenders.H_EMPLOY,
      us_nd_docstars_offenders.I_JOB_CL,
      us_nd_docstars_offenders.J_LAST_GR,
      us_nd_docstars_offenders.K_PUB_ASST,
      us_nd_docstars_offenders.INACTIVEDATE__raw,
      us_nd_docstars_offenders.BIGSIXT1,
      us_nd_docstars_offenders.BIGSIXT2,
      us_nd_docstars_offenders.BIGSIXT3,
      us_nd_docstars_offenders.BIGSIXT4,
      us_nd_docstars_offenders.BIGSIXT5,
      us_nd_docstars_offenders.BIGSIXT6,
      us_nd_docstars_offenders.ACTIVEREVOCATION_IND,
      us_nd_docstars_offenders.LSITOTAL,
      us_nd_docstars_offenders.CCCFLAG,
      us_nd_docstars_offenders.RecDate__raw,
      us_nd_docstars_offenders.SORAC_SCORE,
      us_nd_docstars_offenders.HOMELESS,
      us_nd_docstars_offenders.CREATED_BY,
      us_nd_docstars_offenders.RECORDCRDATE__raw,
      us_nd_docstars_offenders.LAST_HOME_VISIT__raw,
      us_nd_docstars_offenders.LAST_FACE_TO_FACE__raw,
      us_nd_docstars_offenders.MAILING_ADDRESS2,
      us_nd_docstars_offenders.PHYSICAL_ADDRESS2,
      us_nd_docstars_offenders.COUNTY_RESIDENCE,
      us_nd_docstars_offenders.EARLY_TERMINATION_DATE__raw,
      us_nd_docstars_offenders.EARLY_TERMINATION_ACKNOWLEDGED,
      us_nd_docstars_offenders.PHONE,
      us_nd_docstars_offenders.PHONE2,
      us_nd_docstars_offenders.EMAIL,
      us_nd_docstars_offenders.file_id,
      us_nd_docstars_offenders.is_deleted]
    sorts: [us_nd_docstars_offenders.DOB__raw]
    note_display: hover
    note_text: "Each row represents a single person who has been or is currently under supervision. This contains basic demographic information about the person, as well as a variety of \"roll-up\" fields that Docstars rolls up from other tables into this one to capture the latest state of the field. These include metadata about a person's latest documented needs, latest assessment results, whether they are being revoked, whether they are absconding, supervision level, and more.  SORAC assessment scores are provided as a \"roll-up\" field representing the most recent SORAC assessment for a person under supervision.  Note: this table does not include historical record of this aforementioned data."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 0
    col: 0
    width: 24
    height: 6

  - name: docstars_contacts
    title: docstars_contacts
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_docstars_contacts.primary_key,
      us_nd_docstars_contacts.RecID,
      us_nd_docstars_contacts.SID,
      us_nd_docstars_contacts.CONTACT_CODE,
      us_nd_docstars_contacts.C2,
      us_nd_docstars_contacts.C3,
      us_nd_docstars_contacts.C4,
      us_nd_docstars_contacts.C5,
      us_nd_docstars_contacts.C6,
      us_nd_docstars_contacts.CATEGORY,
      us_nd_docstars_contacts.ORIGINATOR,
      us_nd_docstars_contacts.CONTACT_DATE__raw,
      us_nd_docstars_contacts.RecDate__raw,
      us_nd_docstars_contacts.file_id,
      us_nd_docstars_contacts.is_deleted]
    sorts: [us_nd_docstars_contacts.CONTACT_DATE__raw]
    note_display: hover
    note_text: "Each row represents a contact between a supervision officer and person under supervision. This includes the kinds of contacts that occurred and where/when they took place."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 6
    col: 0
    width: 24
    height: 6

  - name: docstars_ftr_episode
    title: docstars_ftr_episode
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_docstars_ftr_episode.primary_key,
      us_nd_docstars_ftr_episode.EPISODE_ID,
      us_nd_docstars_ftr_episode.SID,
      us_nd_docstars_ftr_episode.PREFERRED_PROVIDER_ID,
      us_nd_docstars_ftr_episode.PREFERRED_LOCATION_ID,
      us_nd_docstars_ftr_episode.COORDINATOR_GUID,
      us_nd_docstars_ftr_episode.ASSIGNED_PROVIDER_ID,
      us_nd_docstars_ftr_episode.LOCATION_ID,
      us_nd_docstars_ftr_episode.CURRENT_NEEDS,
      us_nd_docstars_ftr_episode.IS_CLINICAL_ASSESSMENT,
      us_nd_docstars_ftr_episode.ASSESSMENT_LOCATION,
      us_nd_docstars_ftr_episode.REFERRAL_REASON,
      us_nd_docstars_ftr_episode.STATUS,
      us_nd_docstars_ftr_episode.STATUS_DATE__raw,
      us_nd_docstars_ftr_episode.STRENGTHS,
      us_nd_docstars_ftr_episode.NEEDS,
      us_nd_docstars_ftr_episode.SN_LAST_UPDATED_DATE__raw,
      us_nd_docstars_ftr_episode.SUBMITTED__raw,
      us_nd_docstars_ftr_episode.SUBMITTED_BY,
      us_nd_docstars_ftr_episode.SUBMITTED_BY_NAME,
      us_nd_docstars_ftr_episode.ADMITTED_DATE__raw,
      us_nd_docstars_ftr_episode.ALLOW_VIEWING,
      us_nd_docstars_ftr_episode.PEER_SUPPORT_OFFERED,
      us_nd_docstars_ftr_episode.PEER_SUPPORT_ACCEPTED,
      us_nd_docstars_ftr_episode.SPECIALIST_LAST_NAME,
      us_nd_docstars_ftr_episode.SPECIALIST_FIRST_NAME,
      us_nd_docstars_ftr_episode.SPECIALIST_INITIAL,
      us_nd_docstars_ftr_episode.file_id,
      us_nd_docstars_ftr_episode.is_deleted]
    sorts: [us_nd_docstars_ftr_episode.STATUS_DATE__raw]
    note_display: hover
    note_text: "Each row represents a particular instance of a referral of a particular person to a particular program within FTR (Free Through Recovery). This includes metadata about the referral as well as the program interaction itself."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 12
    col: 0
    width: 24
    height: 6

  - name: docstars_lsi_chronology
    title: docstars_lsi_chronology
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_docstars_lsi_chronology.primary_key,
      us_nd_docstars_lsi_chronology.RecID,
      us_nd_docstars_lsi_chronology.SID,
      us_nd_docstars_lsi_chronology.CHtotal,
      us_nd_docstars_lsi_chronology.Q18value,
      us_nd_docstars_lsi_chronology.Q19value,
      us_nd_docstars_lsi_chronology.Q20value,
      us_nd_docstars_lsi_chronology.EETotal,
      us_nd_docstars_lsi_chronology.Q21value,
      us_nd_docstars_lsi_chronology.FnclTotal,
      us_nd_docstars_lsi_chronology.Q23Value,
      us_nd_docstars_lsi_chronology.Q24Value,
      us_nd_docstars_lsi_chronology.Q25Value,
      us_nd_docstars_lsi_chronology.FMTotal,
      us_nd_docstars_lsi_chronology.Q27Value,
      us_nd_docstars_lsi_chronology.AccomTotal,
      us_nd_docstars_lsi_chronology.Q31Value,
      us_nd_docstars_lsi_chronology.LRTotal,
      us_nd_docstars_lsi_chronology.Cptotal,
      us_nd_docstars_lsi_chronology.Q39Value,
      us_nd_docstars_lsi_chronology.Q40Value,
      us_nd_docstars_lsi_chronology.AdTotal,
      us_nd_docstars_lsi_chronology.EPTotal,
      us_nd_docstars_lsi_chronology.Q51value,
      us_nd_docstars_lsi_chronology.Q52Value,
      us_nd_docstars_lsi_chronology.AOTotal,
      us_nd_docstars_lsi_chronology.LSI_CHARGE,
      us_nd_docstars_lsi_chronology.CREATED_BY,
      us_nd_docstars_lsi_chronology.VERSION_ID,
      us_nd_docstars_lsi_chronology.COMPLETE,
      us_nd_docstars_lsi_chronology.SUPERLEVEL,
      us_nd_docstars_lsi_chronology.AssessmentDate__raw,
      us_nd_docstars_lsi_chronology.LastUpdate,
      us_nd_docstars_lsi_chronology.INACTIVEDATE__raw,
      us_nd_docstars_lsi_chronology.RecDate__raw,
      us_nd_docstars_lsi_chronology.file_id,
      us_nd_docstars_lsi_chronology.is_deleted]
    sorts: [us_nd_docstars_lsi_chronology.AssessmentDate__raw]
    note_display: hover
    note_text: "Each row represents a particular LSIR assessment taken by a particular person under supervision."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 18
    col: 0
    width: 24
    height: 6

  - name: docstars_offendercasestable
    title: docstars_offendercasestable
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_docstars_offendercasestable.primary_key,
      us_nd_docstars_offendercasestable.CASE_NUMBER,
      us_nd_docstars_offendercasestable.SID,
      us_nd_docstars_offendercasestable.SENT_TYPE,
      us_nd_docstars_offendercasestable.DESCRIPTION,
      us_nd_docstars_offendercasestable.SENT_YY,
      us_nd_docstars_offendercasestable.SENT_MM,
      us_nd_docstars_offendercasestable.JUDGE,
      us_nd_docstars_offendercasestable.PAROLE_FR__raw,
      us_nd_docstars_offendercasestable.PAROLE_TO__raw,
      us_nd_docstars_offendercasestable.TERM_DATE__raw,
      us_nd_docstars_offendercasestable.TA_TYPE,
      us_nd_docstars_offendercasestable.TB_CTY,
      us_nd_docstars_offendercasestable.TD_PUB_AST,
      us_nd_docstars_offendercasestable.TE_EMPLOY,
      us_nd_docstars_offendercasestable.TF_RESPNSE,
      us_nd_docstars_offendercasestable.TG_COMMRES,
      us_nd_docstars_offendercasestable.TH_MARITAL,
      us_nd_docstars_offendercasestable.TI_EMPLOY,
      us_nd_docstars_offendercasestable.TJ_INCOME,
      us_nd_docstars_offendercasestable.TK_LAST_GR,
      us_nd_docstars_offendercasestable.TL_LIV_ARR,
      us_nd_docstars_offendercasestable.REV_DATE__raw,
      us_nd_docstars_offendercasestable.NEW_OFF,
      us_nd_docstars_offendercasestable.NEW_OFF2,
      us_nd_docstars_offendercasestable.NEW_OFF3,
      us_nd_docstars_offendercasestable.TERMINATING_OFFICER,
      us_nd_docstars_offendercasestable.REV_NOFF_YN,
      us_nd_docstars_offendercasestable.REV_ABSC_YN,
      us_nd_docstars_offendercasestable.REV_TECH_YN,
      us_nd_docstars_offendercasestable.LAST_UPDATE,
      us_nd_docstars_offendercasestable.INACTIVEDATE__raw,
      us_nd_docstars_offendercasestable.RECORDCRDATE__raw,
      us_nd_docstars_offendercasestable.RevoDispo,
      us_nd_docstars_offendercasestable.RecDate__raw,
      us_nd_docstars_offendercasestable.CHANGE_DATE_REASON,
      us_nd_docstars_offendercasestable.file_id,
      us_nd_docstars_offendercasestable.is_deleted]
    sorts: [us_nd_docstars_offendercasestable.PAROLE_FR__raw]
    note_display: hover
    note_text: "Each row represents a distinct \"case\" for a particular person under supervision, i.e. a period of supervision served by some person for some reason. This includes metadata about the sentence length, the actual period being served, the needs of the person during this period, and whether a revocation has occurred and why."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 24
    col: 0
    width: 24
    height: 6

  - name: docstars_offensestable
    title: docstars_offensestable
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_docstars_offensestable.primary_key,
      us_nd_docstars_offensestable.RecID,
      us_nd_docstars_offensestable.CASE_NUMBER,
      us_nd_docstars_offensestable.SID,
      us_nd_docstars_offensestable.CODE,
      us_nd_docstars_offensestable.Common_Statute_Number,
      us_nd_docstars_offensestable.Common_Statute_NCIC_Code,
      us_nd_docstars_offensestable.LEVEL,
      us_nd_docstars_offensestable.COUNTY,
      us_nd_docstars_offensestable.COURT_NUMBER,
      us_nd_docstars_offensestable.LAST_UPDATE__raw,
      us_nd_docstars_offensestable.COUNTS,
      us_nd_docstars_offensestable.INACTIVEDATE__raw,
      us_nd_docstars_offensestable.OFFENSEDATE__raw,
      us_nd_docstars_offensestable.RecDate__raw,
      us_nd_docstars_offensestable.YEAR,
      us_nd_docstars_offensestable.CREATED_BY,
      us_nd_docstars_offensestable.MASTER_OFFENSE_IND,
      us_nd_docstars_offensestable.COUNT,
      us_nd_docstars_offensestable.REQUIRES_REGISTRATION,
      us_nd_docstars_offensestable.file_id,
      us_nd_docstars_offensestable.is_deleted]
    sorts: [us_nd_docstars_offensestable.INACTIVEDATE__raw]
    note_display: hover
    note_text: "Each row represents a particular offense that a person is being charged with, leading to a sentence to supervision."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 30
    col: 0
    width: 24
    height: 6

  - name: elite_offenderidentifier
    title: elite_offenderidentifier
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offenderidentifier.primary_key,
      us_nd_elite_offenderidentifier.ROOT_OFFENDER_ID,
      us_nd_elite_offenderidentifier.IDENTIFIER_TYPE,
      us_nd_elite_offenderidentifier.IDENTIFIER,
      us_nd_elite_offenderidentifier.CREATE_DATETIME__raw,
      us_nd_elite_offenderidentifier.MODIFY_DATETIME__raw,
      us_nd_elite_offenderidentifier.file_id,
      us_nd_elite_offenderidentifier.is_deleted]
    sorts: [us_nd_elite_offenderidentifier.CREATE_DATETIME__raw]
    note_display: hover
    note_text: "Each row represents some unique external identifier provided by DOCR data systems for a particular person. In effect, this is a DOCR-provided join table to bring together \"Elite offender ids\" with \"Docstars state ids\"."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 36
    col: 0
    width: 24
    height: 6

  - name: elite_offenders
    title: elite_offenders
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offenders.primary_key,
      us_nd_elite_offenders.ROOT_OFFENDER_ID,
      us_nd_elite_offenders.LAST_NAME,
      us_nd_elite_offenders.FIRST_NAME,
      us_nd_elite_offenders.BIRTH_DATE__raw,
      us_nd_elite_offenders.SEX_CODE,
      us_nd_elite_offenders.ALIAS_NAME_TYPE,
      us_nd_elite_offenders.RACE_CODE,
      us_nd_elite_offenders.CREATE_DATETIME__raw,
      us_nd_elite_offenders.MODIFY_DATETIME__raw,
      us_nd_elite_offenders.ModifyDate__raw,
      us_nd_elite_offenders.file_id,
      us_nd_elite_offenders.is_deleted]
    sorts: [us_nd_elite_offenders.BIRTH_DATE__raw]
    note_display: hover
    note_text: "Each row represents a single person who has been or is currently incarcerated. This contains basic demographic information about the person as well as a note about what kind of name (alias) is stored on the record."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 42
    col: 0
    width: 24
    height: 6

  - name: elite_alias
    title: elite_alias
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_alias.primary_key,
      us_nd_elite_alias.ROOT_OFFENDER_ID,
      us_nd_elite_alias.ALIAS_NAME_TYPE,
      us_nd_elite_alias.ALIAS_OFFENDER_ID,
      us_nd_elite_alias.FIRST_NAME,
      us_nd_elite_alias.LAST_NAME,
      us_nd_elite_alias.SUFFIX,
      us_nd_elite_alias.MIDDLE_NAME,
      us_nd_elite_alias.OFFENDER_ID,
      us_nd_elite_alias.RACE_CODE,
      us_nd_elite_alias.SEX_CODE,
      us_nd_elite_alias.CREATE_DATETIME__raw,
      us_nd_elite_alias.MODIFY_DATETIME__raw,
      us_nd_elite_alias.file_id,
      us_nd_elite_alias.is_deleted]
    sorts: [us_nd_elite_alias.CREATE_DATETIME__raw]
    note_display: hover
    note_text: "Each row represents some alias that has been recorded for a particular person who has been incarcerated."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 48
    col: 0
    width: 24
    height: 6

  - name: elite_offenderbookingstable
    title: elite_offenderbookingstable
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offenderbookingstable.primary_key,
      us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID,
      us_nd_elite_offenderbookingstable.ROOT_OFFENDER_ID,
      us_nd_elite_offenderbookingstable.BOOKING_NO,
      us_nd_elite_offenderbookingstable.AGY_LOC_ID,
      us_nd_elite_offenderbookingstable.LIVING_UNIT_ID,
      us_nd_elite_offenderbookingstable.IN_OUT_STATUS,
      us_nd_elite_offenderbookingstable.ACTIVE_FLAG,
      us_nd_elite_offenderbookingstable.BOOKING_STATUS,
      us_nd_elite_offenderbookingstable.BOOKING_TYPE,
      us_nd_elite_offenderbookingstable.BOOKING_BEGIN_DATE__raw,
      us_nd_elite_offenderbookingstable.BOOKING_END_DATE__raw,
      us_nd_elite_offenderbookingstable.CREATE_DATETIME__raw,
      us_nd_elite_offenderbookingstable.MODIFY_DATETIME__raw,
      us_nd_elite_offenderbookingstable.file_id,
      us_nd_elite_offenderbookingstable.is_deleted]
    sorts: [us_nd_elite_offenderbookingstable.BOOKING_BEGIN_DATE__raw]
    note_display: hover
    note_text: "Each row represents a \"booking\" into DOCR, i.e. a new series of interactions with the justice system due to a new offense. For example, a new booking is created when a person has their first ever conviction and sentencing under DOCR jurisdiction, or when a person who was previously incarcerated and is now at libery has a new conviction and sentencing. However, a new booking is not created when a person under supervision is revoked and sentenced back to incarceration.  Each row contains some basic \"roll-up\" fields that are rolled up from other tables to give a snapshot of the current status of the booking."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 54
    col: 0
    width: 24
    height: 6

  - name: elite_offense_in_custody_and_pos_report_data
    title: elite_offense_in_custody_and_pos_report_data
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offense_in_custody_and_pos_report_data.primary_key,
      us_nd_elite_offense_in_custody_and_pos_report_data.ROOT_OFFENDER_ID,
      us_nd_elite_offense_in_custody_and_pos_report_data.LAST_NAME,
      us_nd_elite_offense_in_custody_and_pos_report_data.FIRST_NAME,
      us_nd_elite_offense_in_custody_and_pos_report_data.OFFENDER_BOOK_ID,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_INCIDENT_ID,
      us_nd_elite_offense_in_custody_and_pos_report_data.AGENCY_INCIDENT_ID,
      us_nd_elite_offense_in_custody_and_pos_report_data.INCIDENT_DATE__raw,
      us_nd_elite_offense_in_custody_and_pos_report_data.INCIDENT_TYPE,
      us_nd_elite_offense_in_custody_and_pos_report_data.INCIDENT_TYPE_DESC,
      us_nd_elite_offense_in_custody_and_pos_report_data.INCIDENT_DETAILS,
      us_nd_elite_offense_in_custody_and_pos_report_data.AGY_LOC_ID,
      us_nd_elite_offense_in_custody_and_pos_report_data.OMS_OWNER_V_OIC_INCIDENTS___INT_LOC_DESCRIPTION,
      us_nd_elite_offense_in_custody_and_pos_report_data.REPORT_DATE__raw,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_HEARING_ID,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_HEARING_TYPE,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_HEARING_TYPE_DESC,
      us_nd_elite_offense_in_custody_and_pos_report_data.HEARING_DATE__raw,
      us_nd_elite_offense_in_custody_and_pos_report_data.HEARING_STAFF_NAME,
      us_nd_elite_offense_in_custody_and_pos_report_data.OMS_OWNER_V_OIC_HEARINGS_COMMENT_TEXT,
      us_nd_elite_offense_in_custody_and_pos_report_data.OMS_OWNER_V_OIC_HEARINGS_INT_LOC_DESCRIPTION,
      us_nd_elite_offense_in_custody_and_pos_report_data.OMS_OWNER_V_OIC_HEARING_RESULTS_RESULT_SEQ,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_OFFENCE_CATEGORY,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_OFFENCE_CODE,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_OFFENCE_DESCRIPTION,
      us_nd_elite_offense_in_custody_and_pos_report_data.PLEA_DESCRIPTION,
      us_nd_elite_offense_in_custody_and_pos_report_data.FINDING_DESCRIPTION,
      us_nd_elite_offense_in_custody_and_pos_report_data.RESULT_OIC_OFFENCE_CATEGORY,
      us_nd_elite_offense_in_custody_and_pos_report_data.RESULT_OIC_OFFENCE_CODE,
      us_nd_elite_offense_in_custody_and_pos_report_data.RESULT_OIC_OFFENCE_DESCRIPTION,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_SANCTION_DESC,
      us_nd_elite_offense_in_custody_and_pos_report_data.Expr1030,
      us_nd_elite_offense_in_custody_and_pos_report_data.SANCTION_SEQ,
      us_nd_elite_offense_in_custody_and_pos_report_data.COMPENSATION_AMOUNT,
      us_nd_elite_offense_in_custody_and_pos_report_data.SANCTION_MONTHS,
      us_nd_elite_offense_in_custody_and_pos_report_data.SANCTION_DAYS,
      us_nd_elite_offense_in_custody_and_pos_report_data.OMS_OWNER_V_OFFENDER_OIC_SANCTIONS_COMMENT_TEXT,
      us_nd_elite_offense_in_custody_and_pos_report_data.EFFECTIVE_DATE__raw,
      us_nd_elite_offense_in_custody_and_pos_report_data.OIC_SANCTION_CODE,
      us_nd_elite_offense_in_custody_and_pos_report_data.OMS_OWNER_V_OFFENDER_OIC_SANCTIONS_RESULT_SEQ,
      us_nd_elite_offense_in_custody_and_pos_report_data.ALIAS_NAME_TYPE,
      us_nd_elite_offense_in_custody_and_pos_report_data.file_id,
      us_nd_elite_offense_in_custody_and_pos_report_data.is_deleted]
    sorts: [us_nd_elite_offense_in_custody_and_pos_report_data.INCIDENT_DATE__raw]
    note_display: hover
    note_text: "All data for offenses committed in custody, their results and sanctions attached to each offense."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 60
    col: 0
    width: 24
    height: 6

  - name: recidiviz_elite_offender_alerts
    title: recidiviz_elite_offender_alerts
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_recidiviz_elite_offender_alerts.primary_key,
      us_nd_recidiviz_elite_offender_alerts.ALERT_CODE,
      us_nd_recidiviz_elite_offender_alerts.ALERT_SEQ,
      us_nd_recidiviz_elite_offender_alerts.ALERT_STATUS,
      us_nd_recidiviz_elite_offender_alerts.ALERT_DATE__raw,
      us_nd_recidiviz_elite_offender_alerts.ALERT_TYPE,
      us_nd_recidiviz_elite_offender_alerts.AUTHORIZE_PERSON_TEXT,
      us_nd_recidiviz_elite_offender_alerts.CASELOAD_TYPE,
      us_nd_recidiviz_elite_offender_alerts.COMMENT_TEXT,
      us_nd_recidiviz_elite_offender_alerts.CREATE_DATE__raw,
      us_nd_recidiviz_elite_offender_alerts.CREATE_DATETIME__raw,
      us_nd_recidiviz_elite_offender_alerts.CREATE_USER_ID,
      us_nd_recidiviz_elite_offender_alerts.EXPIRY_DATE__raw,
      us_nd_recidiviz_elite_offender_alerts.MODIFY_DATETIME__raw,
      us_nd_recidiviz_elite_offender_alerts.MODIFY_USER_ID,
      us_nd_recidiviz_elite_offender_alerts.OFFENDER_BOOK_ID,
      us_nd_recidiviz_elite_offender_alerts.ROOT_OFFENDER_ID,
      us_nd_recidiviz_elite_offender_alerts.SEAL_FLAG,
      us_nd_recidiviz_elite_offender_alerts.VERIFIED_FLAG,
      us_nd_recidiviz_elite_offender_alerts.file_id,
      us_nd_recidiviz_elite_offender_alerts.is_deleted]
    sorts: [us_nd_recidiviz_elite_offender_alerts.ALERT_DATE__raw]
    note_display: hover
    note_text: "Alerts sourced from the Elite system."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 66
    col: 0
    width: 24
    height: 6

  - name: elite_bedassignmenthistory
    title: elite_bedassignmenthistory
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_bedassignmenthistory.primary_key,
      us_nd_elite_bedassignmenthistory.OFFENDER_BOOK_ID,
      us_nd_elite_bedassignmenthistory.LIVING_UNIT_ID,
      us_nd_elite_bedassignmenthistory.ASSIGNMENT_DATE__raw,
      us_nd_elite_bedassignmenthistory.ASSIGNMENT_END_DATE__raw,
      us_nd_elite_bedassignmenthistory.BED_ASSIGN_SEQ,
      us_nd_elite_bedassignmenthistory.CREATE_DATETIME__raw,
      us_nd_elite_bedassignmenthistory.MODIFY_DATETIME__raw,
      us_nd_elite_bedassignmenthistory.file_id,
      us_nd_elite_bedassignmenthistory.is_deleted]
    sorts: [us_nd_elite_bedassignmenthistory.ASSIGNMENT_DATE__raw]
    note_display: hover
    note_text: "Each row represents a particular stay of a particular person in a particular bed in a particular incarceration facility."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 72
    col: 0
    width: 24
    height: 6

  - name: elite_externalmovements
    title: elite_externalmovements
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_externalmovements.primary_key,
      us_nd_elite_externalmovements.OFFENDER_BOOK_ID,
      us_nd_elite_externalmovements.ACTIVE_FLAG,
      us_nd_elite_externalmovements.ARREST_AGENCY_LOC_ID,
      us_nd_elite_externalmovements.COMMENT_TEXT,
      us_nd_elite_externalmovements.DIRECTION_CODE,
      us_nd_elite_externalmovements.ESCORT_CODE,
      us_nd_elite_externalmovements.FROM_AGY_LOC_ID,
      us_nd_elite_externalmovements.MOVEMENT_SEQ,
      us_nd_elite_externalmovements.MOVEMENT_TYPE,
      us_nd_elite_externalmovements.MOVEMENT_REASON_CODE,
      us_nd_elite_externalmovements.MOVEMENT_DATE__raw,
      us_nd_elite_externalmovements.TO_AGY_LOC_ID,
      us_nd_elite_externalmovements.CREATE_DATETIME__raw,
      us_nd_elite_externalmovements.MODIFY_DATETIME__raw,
      us_nd_elite_externalmovements.file_id,
      us_nd_elite_externalmovements.is_deleted]
    sorts: [us_nd_elite_externalmovements.MOVEMENT_DATE__raw]
    note_display: hover
    note_text: "Each row represents a single movement of an incarcerated person from one incarceration facility to another, or in some cases to a different institution or to the outside world in the case of release. Sequences of rows in this table, for a given person and ordered by the MOVEMENT_SEQ, can be used to identify periods of incarceration."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 78
    col: 0
    width: 24
    height: 6

  - name: elite_institutionalactivities
    title: elite_institutionalactivities
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_institutionalactivities.primary_key,
      us_nd_elite_institutionalactivities.OFFENDER_BOOK_ID,
      us_nd_elite_institutionalactivities.DESCRIPTION,
      us_nd_elite_institutionalactivities.PROGRAM_CODE,
      us_nd_elite_institutionalactivities.START_DATE__raw,
      us_nd_elite_institutionalactivities.END_DATE__raw,
      us_nd_elite_institutionalactivities.OFFENDER_END_REASON,
      us_nd_elite_institutionalactivities.OFFENDER_END_COMMENT_TEXT,
      us_nd_elite_institutionalactivities.REFERRAL_DATE__raw,
      us_nd_elite_institutionalactivities.PROGRAM_ID,
      us_nd_elite_institutionalactivities.CREATE_DATETIME__raw,
      us_nd_elite_institutionalactivities.MODIFY_DATETIME__raw,
      us_nd_elite_institutionalactivities.OFFENDER_PROGRAM_STATUS,
      us_nd_elite_institutionalactivities.REFERRAL_PRIORITY,
      us_nd_elite_institutionalactivities.REFERRAL_COMMENT_TEXT,
      us_nd_elite_institutionalactivities.REJECT_DATE__raw,
      us_nd_elite_institutionalactivities.REJECT_REASON_CODE,
      us_nd_elite_institutionalactivities.AGY_LOC_ID,
      us_nd_elite_institutionalactivities.COURSE_ACTIVITIES_DESCRIPTION,
      us_nd_elite_institutionalactivities.file_id,
      us_nd_elite_institutionalactivities.is_deleted]
    sorts: [us_nd_elite_institutionalactivities.START_DATE__raw]
    note_display: hover
    note_text: "Each row represents some activity (e.g. work assignment) or program (e.g. mental health treatment, educational program) undertaken by an incarcerated person. This includes metadata about the activity/program itself, when it was undertaken, their referral into the program, and free text notes about the person's participation.  Importantly, DOCR has itself noted that the trustworthiness of this data is questionable, as it is a bit of a dumping ground for a lot of only loosely affiliated kinds of data and as it is not commonly kept up-to-date, i.e. many rows indicating that programs or activities from many, many years ago are still ongoing."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 84
    col: 0
    width: 24
    height: 6

  - name: elite_offendersentenceaggs
    title: elite_offendersentenceaggs
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offendersentenceaggs.primary_key,
      us_nd_elite_offendersentenceaggs.OFFENDER_BOOK_ID,
      us_nd_elite_offendersentenceaggs.MAX_TERM,
      us_nd_elite_offendersentenceaggs.EARLIEST_SENT_START_DATE__raw,
      us_nd_elite_offendersentenceaggs.FINAL_SENT_EXP_DATE__raw,
      us_nd_elite_offendersentenceaggs.CALC_POS_REL_DATE__raw,
      us_nd_elite_offendersentenceaggs.OVR_POS_REL_DATE__raw,
      us_nd_elite_offendersentenceaggs.PAROLE_DATE__raw,
      us_nd_elite_offendersentenceaggs.PAROLE_REVIEW_DATE__raw,
      us_nd_elite_offendersentenceaggs.CREATE_DATETIME__raw,
      us_nd_elite_offendersentenceaggs.MODIFY_DATETIME__raw,
      us_nd_elite_offendersentenceaggs.file_id,
      us_nd_elite_offendersentenceaggs.is_deleted]
    sorts: [us_nd_elite_offendersentenceaggs.EARLIEST_SENT_START_DATE__raw]
    note_display: hover
    note_text: "Each row represents aggregated (\"rolled up\") information about the collection of sentences associated with a single booking. This includes the expected eventual release date based on all of the aggregated sentence information, when the person is expected to receive parole, and so on."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 90
    col: 0
    width: 24
    height: 6

  - name: elite_offendersentences
    title: elite_offendersentences
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offendersentences.primary_key,
      us_nd_elite_offendersentences.OFFENDER_BOOK_ID,
      us_nd_elite_offendersentences.CHARGE_SEQ,
      us_nd_elite_offendersentences.CONSEC_TO_SENTENCE_SEQ,
      us_nd_elite_offendersentences.COMMENT_TEXT,
      us_nd_elite_offendersentences.CONSECUTIVE_COUNT_FLAG,
      us_nd_elite_offendersentences.COUNTS,
      us_nd_elite_offendersentences.GOOD_TIME,
      us_nd_elite_offendersentences.SENTENCE_CALC_TYPE,
      us_nd_elite_offendersentences.SENTENCE_SEQ,
      us_nd_elite_offendersentences.SENTENCE_STATUS,
      us_nd_elite_offendersentences.EFFECTIVE_DATE__raw,
      us_nd_elite_offendersentences.EIGHTYFIVE_PERCENT_DATE__raw,
      us_nd_elite_offendersentences.PROBABLE_RELEASE_DATE__raw,
      us_nd_elite_offendersentences.SENTENCE_EXPIRY_DATE__raw,
      us_nd_elite_offendersentences.START_DATE__raw,
      us_nd_elite_offendersentences.CREATE_DATETIME__raw,
      us_nd_elite_offendersentences.MODIFY_DATETIME__raw,
      us_nd_elite_offendersentences.file_id,
      us_nd_elite_offendersentences.is_deleted]
    sorts: [us_nd_elite_offendersentences.EFFECTIVE_DATE__raw]
    note_display: hover
    note_text: "Each row represents a single sentence of incarceration handed down to a particular person as part of some booking. This includes date information outlining when the sentence is to be served and for how long as well as metadata about how this sentence relates to other sentences. There is a one-to-many relationship from bookings to sentences, and therefore some sentences can finish before the booking end date. However, since ND maintains incarceration and supervision sentences in separate systems (Elite vs. Docstars) all Elite sentences are considered completed by the booking end date, at the latest."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 96
    col: 0
    width: 24
    height: 6

  - name: elite_offendersentenceterms
    title: elite_offendersentenceterms
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_offendersentenceterms.primary_key,
      us_nd_elite_offendersentenceterms.OFFENDER_BOOK_ID,
      us_nd_elite_offendersentenceterms.START_DATE__raw,
      us_nd_elite_offendersentenceterms.END_DATE__raw,
      us_nd_elite_offendersentenceterms.YEARS,
      us_nd_elite_offendersentenceterms.MONTHS,
      us_nd_elite_offendersentenceterms.DAYS,
      us_nd_elite_offendersentenceterms.CREATE_DATETIME__raw,
      us_nd_elite_offendersentenceterms.MODIFY_DATETIME__raw,
      us_nd_elite_offendersentenceterms.SENTENCE_SEQ,
      us_nd_elite_offendersentenceterms.TERM_SEQ,
      us_nd_elite_offendersentenceterms.SENTENCE_TERM_CODE,
      us_nd_elite_offendersentenceterms.file_id,
      us_nd_elite_offendersentenceterms.is_deleted]
    sorts: [us_nd_elite_offendersentenceterms.START_DATE__raw]
    note_display: hover
    note_text: "Each row represents one set of terms for a single sentence of incarceration handed down to a particular person. These terms are essentially specific instances of a planned incarceration stay resulting from the sentence, of which there could potentially be several for a single sentence. There is a one-to-many relationship from an incarceration sentence to sentence terms."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 102
    col: 0
    width: 24
    height: 6

  - name: elite_orderstable
    title: elite_orderstable
    explore: us_nd_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_nd_elite_orderstable.primary_key,
      us_nd_elite_orderstable.ORDER_ID,
      us_nd_elite_orderstable.OFFENDER_BOOK_ID,
      us_nd_elite_orderstable.CONVICTION_DATE__raw,
      us_nd_elite_orderstable.COUNTY_CODE,
      us_nd_elite_orderstable.COURT_DATE__raw,
      us_nd_elite_orderstable.COURT_INFO_ID,
      us_nd_elite_orderstable.EFFECTIVE_DATE__raw,
      us_nd_elite_orderstable.JUDGE_NAME,
      us_nd_elite_orderstable.ORDER_STATUS,
      us_nd_elite_orderstable.SENTENCE_START_DATE__raw,
      us_nd_elite_orderstable.ISSUING_AGY_LOC_ID,
      us_nd_elite_orderstable.CREATE_DATETIME__raw,
      us_nd_elite_orderstable.MODIFY_DATETIME__raw,
      us_nd_elite_orderstable.ORDER_TYPE,
      us_nd_elite_orderstable.file_id,
      us_nd_elite_orderstable.is_deleted]
    sorts: [us_nd_elite_orderstable.CONVICTION_DATE__raw]
    note_display: hover
    note_text: "Each row represents a single court order, and the court case it comes from, that has been or is going to be handed down for some set of charges. This includes metadata about the court case itself as well as the results of the order."
    listen: 
      View Type: us_nd_docstars_offenders.view_type
      US_ND_SID: us_nd_docstars_offenders.SID
      US_ND_ELITE_BOOKING: us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID
      US_ND_ELITE: us_nd_elite_offenders.ROOT_OFFENDER_ID
    row: 108
    col: 0
    width: 24
    height: 6

