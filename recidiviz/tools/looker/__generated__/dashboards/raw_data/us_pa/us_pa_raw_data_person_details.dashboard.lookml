# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_pa_raw_data_person_details
  title: Pennsylvania Raw Data Person Details
  description: For examining individuals in US_PA's raw data tables
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
    explore: us_pa_raw_data
    field: us_pa_dbo_tblSearchInmateInfo.view_type

  - name: US_PA_INMATE
    title: US_PA_INMATE
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_pa_raw_data
    field: us_pa_dbo_tblSearchInmateInfo.inmate_number

  - name: US_PA_CONT
    title: US_PA_CONT
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_pa_raw_data
    field: us_pa_dbo_tblSearchInmateInfo.control_number

  - name: US_PA_PBPP
    title: US_PA_PBPP
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_pa_raw_data
    field: us_pa_dbo_tblSearchInmateInfo.parole_board_num

  elements:
  - name: dbo_tblSearchInmateInfo
    title: dbo_tblSearchInmateInfo
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_tblSearchInmateInfo.primary_key,
      us_pa_dbo_tblSearchInmateInfo.inmate_number,
      us_pa_dbo_tblSearchInmateInfo.control_number,
      us_pa_dbo_tblSearchInmateInfo.state_id_num,
      us_pa_dbo_tblSearchInmateInfo.state_id_num_hashed,
      us_pa_dbo_tblSearchInmateInfo.location_in_transi,
      us_pa_dbo_tblSearchInmateInfo.location_temporary,
      us_pa_dbo_tblSearchInmateInfo.location_permanent,
      us_pa_dbo_tblSearchInmateInfo.parole_board_num,
      us_pa_dbo_tblSearchInmateInfo.event_number,
      us_pa_dbo_tblSearchInmateInfo.event_date__raw,
      us_pa_dbo_tblSearchInmateInfo.delete_date__raw,
      us_pa_dbo_tblSearchInmateInfo.reception_date__raw,
      us_pa_dbo_tblSearchInmateInfo.photo_reason,
      us_pa_dbo_tblSearchInmateInfo.date_of_birth,
      us_pa_dbo_tblSearchInmateInfo.ssn_1,
      us_pa_dbo_tblSearchInmateInfo.re_user_id,
      us_pa_dbo_tblSearchInmateInfo.ic_prv_off_cde_1,
      us_pa_dbo_tblSearchInmateInfo.re_prev_off_cd_1,
      us_pa_dbo_tblSearchInmateInfo.ic_prv_off_cde_2,
      us_pa_dbo_tblSearchInmateInfo.re_prev_off_cd_2,
      us_pa_dbo_tblSearchInmateInfo.ic_prv_off_cde_3,
      us_pa_dbo_tblSearchInmateInfo.re_prev_off_cd_3,
      us_pa_dbo_tblSearchInmateInfo.temp_custody,
      us_pa_dbo_tblSearchInmateInfo.custody,
      us_pa_dbo_tblSearchInmateInfo.temp_p_code_2,
      us_pa_dbo_tblSearchInmateInfo.temp_p_code_1,
      us_pa_dbo_tblSearchInmateInfo.program_level_1,
      us_pa_dbo_tblSearchInmateInfo.program_level_2,
      us_pa_dbo_tblSearchInmateInfo.temp_p_code_3,
      us_pa_dbo_tblSearchInmateInfo.program_level_3,
      us_pa_dbo_tblSearchInmateInfo.phila_photo_num,
      us_pa_dbo_tblSearchInmateInfo.pitts_photo_num,
      us_pa_dbo_tblSearchInmateInfo.fbi_num,
      us_pa_dbo_tblSearchInmateInfo.problematic_offenses,
      us_pa_dbo_tblSearchInmateInfo.race_code,
      us_pa_dbo_tblSearchInmateInfo.sex_type,
      us_pa_dbo_tblSearchInmateInfo.commit_cnty,
      us_pa_dbo_tblSearchInmateInfo.marital_status_code,
      us_pa_dbo_tblSearchInmateInfo.religion_code,
      us_pa_dbo_tblSearchInmateInfo.sent_date__raw,
      us_pa_dbo_tblSearchInmateInfo.min_expir_date__raw,
      us_pa_dbo_tblSearchInmateInfo.min_cort_sent_yrs,
      us_pa_dbo_tblSearchInmateInfo.min_cort_sent_mths,
      us_pa_dbo_tblSearchInmateInfo.min_cort_sent_days,
      us_pa_dbo_tblSearchInmateInfo.min_cort_sent_l_da,
      us_pa_dbo_tblSearchInmateInfo.class_of_sent,
      us_pa_dbo_tblSearchInmateInfo.max_expir_date,
      us_pa_dbo_tblSearchInmateInfo.max_cort_sent_yrs,
      us_pa_dbo_tblSearchInmateInfo.max_cort_sent_mths,
      us_pa_dbo_tblSearchInmateInfo.max_cort_sent_days,
      us_pa_dbo_tblSearchInmateInfo.max_cort_sent_l_da,
      us_pa_dbo_tblSearchInmateInfo.type_number,
      us_pa_dbo_tblSearchInmateInfo.offense_code,
      us_pa_dbo_tblSearchInmateInfo.parole_status_cde,
      us_pa_dbo_tblSearchInmateInfo.sent_status_code,
      us_pa_dbo_tblSearchInmateInfo.regular_date__raw,
      us_pa_dbo_tblSearchInmateInfo.race,
      us_pa_dbo_tblSearchInmateInfo.sex,
      us_pa_dbo_tblSearchInmateInfo.cnty_name,
      us_pa_dbo_tblSearchInmateInfo.offense,
      us_pa_dbo_tblSearchInmateInfo.sentence_status,
      us_pa_dbo_tblSearchInmateInfo.parole_status,
      us_pa_dbo_tblSearchInmateInfo.marital_status,
      us_pa_dbo_tblSearchInmateInfo.sentence_class,
      us_pa_dbo_tblSearchInmateInfo.religion,
      us_pa_dbo_tblSearchInmateInfo.photo_desc,
      us_pa_dbo_tblSearchInmateInfo.facbed_building,
      us_pa_dbo_tblSearchInmateInfo.facbed_section,
      us_pa_dbo_tblSearchInmateInfo.facbed_level,
      us_pa_dbo_tblSearchInmateInfo.facbed_cell_dorm,
      us_pa_dbo_tblSearchInmateInfo.facbed_bed_num,
      us_pa_dbo_tblSearchInmateInfo.max_class_of_sent,
      us_pa_dbo_tblSearchInmateInfo.max_sentence_class,
      us_pa_dbo_tblSearchInmateInfo.SSN_2,
      us_pa_dbo_tblSearchInmateInfo.rcptpn_regular_date__raw,
      us_pa_dbo_tblSearchInmateInfo.Dtn_SW,
      us_pa_dbo_tblSearchInmateInfo.Lst_Nm,
      us_pa_dbo_tblSearchInmateInfo.Frst_Nm,
      us_pa_dbo_tblSearchInmateInfo.Mid_Nm,
      us_pa_dbo_tblSearchInmateInfo.Nm_Suff,
      us_pa_dbo_tblSearchInmateInfo.CurrLoc_Cd,
      us_pa_dbo_tblSearchInmateInfo.RecmpMax_Dt__raw,
      us_pa_dbo_tblSearchInmateInfo.Reclass_Dt__raw,
      us_pa_dbo_tblSearchInmateInfo.IC_Userid,
      us_pa_dbo_tblSearchInmateInfo.IC_Dt__raw,
      us_pa_dbo_tblSearchInmateInfo.Move_Add,
      us_pa_dbo_tblSearchInmateInfo.file_id,
      us_pa_dbo_tblSearchInmateInfo.is_deleted]
    sorts: [us_pa_dbo_tblSearchInmateInfo.event_date__raw]
    note_display: hover
    note_text: "A table that acts as a \"roll-up\" for various bits of information about a person who is currently or was previously incarcerated under the authority of PADOC. It consolidates information from a variety of tables to provide a single interface to understanding the status of a person.  Because Recidiviz ingests the underlying tables directly, Recidiviz uses this table specifically to tie together identifiers that otherwise are difficult to tie together, using it as a reference table. This permits the ability to reliably match people records which are located in DOC tables with people records which are located in PBPP tables."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 0
    col: 0
    width: 24
    height: 6

  - name: RECIDIVIZ_REFERENCE_control_number_linking_ids
    title: RECIDIVIZ_REFERENCE_control_number_linking_ids
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.primary_key,
      us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.control_number,
      us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.pseudo_linking_id,
      us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.file_id,
      us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.is_deleted]
    sorts: [us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.control_number, us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.pseudo_linking_id]
    note_display: hover
    note_text: "A table containing a Recidiviz-generated pseudo-id that links PADOC control numbers that have been used for the same person."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 6
    col: 0
    width: 24
    height: 6

  - name: dbo_BoardAction
    title: dbo_BoardAction
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_BoardAction.primary_key,
      us_pa_dbo_BoardAction.ParoleNumber,
      us_pa_dbo_BoardAction.ParoleCountID,
      us_pa_dbo_BoardAction.BdActionID,
      us_pa_dbo_BoardAction.BdActInstitution,
      us_pa_dbo_BoardAction.BdActInstNumber,
      us_pa_dbo_BoardAction.BdActSubInstitution,
      us_pa_dbo_BoardAction.BdActDateMonth,
      us_pa_dbo_BoardAction.BdActDateDay,
      us_pa_dbo_BoardAction.BdActDateYear,
      us_pa_dbo_BoardAction.BdActEntryDateMonth,
      us_pa_dbo_BoardAction.BdActEntryDateDay,
      us_pa_dbo_BoardAction.BdActEntryDateYear,
      us_pa_dbo_BoardAction.BdActInitial,
      us_pa_dbo_BoardAction.BdActAdmin,
      us_pa_dbo_BoardAction.BdActMaxDateYear,
      us_pa_dbo_BoardAction.BdActMaxDateMonth,
      us_pa_dbo_BoardAction.BdActMaxDateDay,
      us_pa_dbo_BoardAction.BdActIsWarehouse,
      us_pa_dbo_BoardAction.BdActOffLastName,
      us_pa_dbo_BoardAction.BdActOffFirstName,
      us_pa_dbo_BoardAction.BdActOffMidName,
      us_pa_dbo_BoardAction.BdActOffSuffix,
      us_pa_dbo_BoardAction.BdActDOPrint,
      us_pa_dbo_BoardAction.BdActDOPrintDate__raw,
      us_pa_dbo_BoardAction.file_id,
      us_pa_dbo_BoardAction.is_deleted]
    sorts: [us_pa_dbo_BoardAction.BdActDOPrintDate__raw]
    note_display: hover
    note_text: "A table describing actions taken by the Pennsylvania Board of Parole and Probation in response to some action taken by or charge levied against a person under supervision, e.g. responses to supervision violations."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 12
    col: 0
    width: 24
    height: 6

  - name: dbo_ConditionCode
    title: dbo_ConditionCode
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_ConditionCode.primary_key,
      us_pa_dbo_ConditionCode.ParoleNumber,
      us_pa_dbo_ConditionCode.ParoleCountID,
      us_pa_dbo_ConditionCode.BdActionID,
      us_pa_dbo_ConditionCode.ConditionCodeID,
      us_pa_dbo_ConditionCode.CndConditionCode,
      us_pa_dbo_ConditionCode.file_id,
      us_pa_dbo_ConditionCode.is_deleted]
    sorts: [us_pa_dbo_ConditionCode.ParoleNumber, us_pa_dbo_ConditionCode.ParoleCountID, us_pa_dbo_ConditionCode.BdActionID, us_pa_dbo_ConditionCode.ConditionCodeID]
    note_display: hover
    note_text: "A table describing specific conditions of supervision that must be abided by for a particular stint under supervision. Specifically, these are conditions of a particular stint of supervision that were imposed as part of a particular board action, which may have been simply the initial imposition of a supervision sentence or a later action taken in response to, say, a supervision violation."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 18
    col: 0
    width: 24
    height: 6

  - name: dbo_ConditionCodeDescription
    title: dbo_ConditionCodeDescription
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_ConditionCodeDescription.primary_key,
      us_pa_dbo_ConditionCodeDescription.ParoleNumber,
      us_pa_dbo_ConditionCodeDescription.ParoleCountID,
      us_pa_dbo_ConditionCodeDescription.BdActionID,
      us_pa_dbo_ConditionCodeDescription.ConditionCodeID,
      us_pa_dbo_ConditionCodeDescription.CndDescriptionID,
      us_pa_dbo_ConditionCodeDescription.ConditionDescription,
      us_pa_dbo_ConditionCodeDescription.TimeStamp__raw,
      us_pa_dbo_ConditionCodeDescription.file_id,
      us_pa_dbo_ConditionCodeDescription.is_deleted]
    sorts: [us_pa_dbo_ConditionCodeDescription.TimeStamp__raw]
    note_display: hover
    note_text: "An addendum to dbo_ConditionCode which adds free text descriptions of what exactly the supervision condition entails and why it was imposed."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 24
    col: 0
    width: 24
    height: 6

  - name: dbo_DOB
    title: dbo_DOB
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_DOB.primary_key,
      us_pa_dbo_DOB.ParoleNumber,
      us_pa_dbo_DOB.DOBID,
      us_pa_dbo_DOB.BirthStateID,
      us_pa_dbo_DOB.DOBYear,
      us_pa_dbo_DOB.DOBMonth,
      us_pa_dbo_DOB.DOBDay,
      us_pa_dbo_DOB.file_id,
      us_pa_dbo_DOB.is_deleted]
    sorts: [us_pa_dbo_DOB.ParoleNumber, us_pa_dbo_DOB.DOBID]
    note_display: hover
    note_text: "A table containing information about dates of birth for people ever supervised by PADOC. NOT REFRESHED REGULARLY."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 30
    col: 0
    width: 24
    height: 6

  - name: dbo_DeActivateParoleNumber
    title: dbo_DeActivateParoleNumber
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_DeActivateParoleNumber.primary_key,
      us_pa_dbo_DeActivateParoleNumber.ParoleNumber,
      us_pa_dbo_DeActivateParoleNumber.InActiveParoleNumber,
      us_pa_dbo_DeActivateParoleNumber.file_id,
      us_pa_dbo_DeActivateParoleNumber.is_deleted]
    sorts: [us_pa_dbo_DeActivateParoleNumber.ParoleNumber]
    note_display: hover
    note_text: "A table containing information about supervision identifiers that have been deactivated by PBPP along with the new idtentifierthat should be used instead."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 36
    col: 0
    width: 24
    height: 6

  - name: dbo_Hist_Parolee
    title: dbo_Hist_Parolee
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Hist_Parolee.primary_key,
      us_pa_dbo_Hist_Parolee.ParoleNumber,
      us_pa_dbo_Hist_Parolee.Release_Date__raw,
      us_pa_dbo_Hist_Parolee.DOBYear,
      us_pa_dbo_Hist_Parolee.DOBMonth,
      us_pa_dbo_Hist_Parolee.DOBDay,
      us_pa_dbo_Hist_Parolee.Last_Name,
      us_pa_dbo_Hist_Parolee.First_Name,
      us_pa_dbo_Hist_Parolee.Middle_Name,
      us_pa_dbo_Hist_Parolee.Suffix,
      us_pa_dbo_Hist_Parolee.Alias,
      us_pa_dbo_Hist_Parolee.Agent_Name_and_Badge,
      us_pa_dbo_Hist_Parolee.sex_type,
      us_pa_dbo_Hist_Parolee.Race,
      us_pa_dbo_Hist_Parolee.ParoleCountID,
      us_pa_dbo_Hist_Parolee.file_id,
      us_pa_dbo_Hist_Parolee.is_deleted]
    sorts: [us_pa_dbo_Hist_Parolee.Release_Date__raw]
    note_display: hover
    note_text: "A table containing information for JII on parole from 1/1/2022 through 4/10/2024.  This is a  one time transfer that was provided by PA since we stopped getting the dbo_Parolee table in 2022, and we have just started getting it again in 2024"
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 42
    col: 0
    width: 24
    height: 6

  - name: dbo_Hist_Release
    title: dbo_Hist_Release
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Hist_Release.primary_key,
      us_pa_dbo_Hist_Release.ParoleNumber,
      us_pa_dbo_Hist_Release.HReleaseID,
      us_pa_dbo_Hist_Release.ParoleCountID,
      us_pa_dbo_Hist_Release.HReOffenderHisName,
      us_pa_dbo_Hist_Release.HReRace,
      us_pa_dbo_Hist_Release.HReSex,
      us_pa_dbo_Hist_Release.HReEmpstat,
      us_pa_dbo_Hist_Release.HReCmcCode,
      us_pa_dbo_Hist_Release.HReRelFrom,
      us_pa_dbo_Hist_Release.HReCntyRes,
      us_pa_dbo_Hist_Release.HReDo,
      us_pa_dbo_Hist_Release.HReOffense,
      us_pa_dbo_Hist_Release.HReReldate,
      us_pa_dbo_Hist_Release.HReMaxDate__raw,
      us_pa_dbo_Hist_Release.HReSidNo,
      us_pa_dbo_Hist_Release.HReAgentName,
      us_pa_dbo_Hist_Release.HReSSNo,
      us_pa_dbo_Hist_Release.HReTransDate__raw,
      us_pa_dbo_Hist_Release.HReTprocDate__raw,
      us_pa_dbo_Hist_Release.HRePurgeCode,
      us_pa_dbo_Hist_Release.HReDOB__raw,
      us_pa_dbo_Hist_Release.HReEntryCode,
      us_pa_dbo_Hist_Release.HReAcceptDate__raw,
      us_pa_dbo_Hist_Release.HReEprocDate__raw,
      us_pa_dbo_Hist_Release.HReStatcode,
      us_pa_dbo_Hist_Release.HReStatDate__raw,
      us_pa_dbo_Hist_Release.HReSprocDate__raw,
      us_pa_dbo_Hist_Release.HReGradeSup,
      us_pa_dbo_Hist_Release.HReGradeSupDt,
      us_pa_dbo_Hist_Release.HReGprProcDate__raw,
      us_pa_dbo_Hist_Release.HReOtna,
      us_pa_dbo_Hist_Release.HReMina,
      us_pa_dbo_Hist_Release.HReMaxa,
      us_pa_dbo_Hist_Release.HReOffa,
      us_pa_dbo_Hist_Release.HReOtnb,
      us_pa_dbo_Hist_Release.HReMinb,
      us_pa_dbo_Hist_Release.HReMaxb,
      us_pa_dbo_Hist_Release.HReOffb,
      us_pa_dbo_Hist_Release.HReOtnc,
      us_pa_dbo_Hist_Release.HReMinc,
      us_pa_dbo_Hist_Release.HReMaxc,
      us_pa_dbo_Hist_Release.HReOffc,
      us_pa_dbo_Hist_Release.HReDelCode,
      us_pa_dbo_Hist_Release.HReDelDate__raw,
      us_pa_dbo_Hist_Release.HReDProcDate__raw,
      us_pa_dbo_Hist_Release.HReRisk,
      us_pa_dbo_Hist_Release.HReNeeds,
      us_pa_dbo_Hist_Release.HReAssessDt,
      us_pa_dbo_Hist_Release.HReEmpDt,
      us_pa_dbo_Hist_Release.HReOveride,
      us_pa_dbo_Hist_Release.HReWant,
      us_pa_dbo_Hist_Release.temp_HReAgentName,
      us_pa_dbo_Hist_Release.HSupervisor,
      us_pa_dbo_Hist_Release.HRelWeight,
      us_pa_dbo_Hist_Release.HOrgCode,
      us_pa_dbo_Hist_Release.HRelDelCodeID,
      us_pa_dbo_Hist_Release.LastModifiedBy,
      us_pa_dbo_Hist_Release.LastModifiedDatetime__raw,
      us_pa_dbo_Hist_Release.HRskAct97Positive,
      us_pa_dbo_Hist_Release.HRskAct97ViolationOfSbstanceAct,
      us_pa_dbo_Hist_Release.HRskAct97DrugRelatedOffense,
      us_pa_dbo_Hist_Release.HRskUrinalysis,
      us_pa_dbo_Hist_Release.HRskBoardMandatedUrinala,
      us_pa_dbo_Hist_Release.HRskAct97Other,
      us_pa_dbo_Hist_Release.file_id,
      us_pa_dbo_Hist_Release.is_deleted]
    sorts: [us_pa_dbo_Hist_Release.HReMaxDate__raw]
    note_display: hover
    note_text: "A table describing specific periods of supervision that were served in the past by a particular person under the authority of PADOC. Release in this context refers to the release of a person from incarceration to supervision, though a minority of these rows include, for example, stints of probation being served in lieu of incarceration.  This table is the historical version tying together all of `dbo_Release`, `dbo_ReleaseInfo`, `dbo_RelStatus`, and `dbo_RelEmployment`. When a period of supervision is terminated and the person is now at liberty or has been convicted of an entirely new offense with entirely new and separate sentences, the records from the aforementioned tables are consolidated together into a row in this table."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 48
    col: 0
    width: 24
    height: 6

  - name: dbo_Hist_SanctionTracking
    title: dbo_Hist_SanctionTracking
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Hist_SanctionTracking.primary_key,
      us_pa_dbo_Hist_SanctionTracking.ParoleNumber,
      us_pa_dbo_Hist_SanctionTracking.ParoleCountID,
      us_pa_dbo_Hist_SanctionTracking.SetID,
      us_pa_dbo_Hist_SanctionTracking.SequenceID,
      us_pa_dbo_Hist_SanctionTracking.Type,
      us_pa_dbo_Hist_SanctionTracking.SanctionCode,
      us_pa_dbo_Hist_SanctionTracking.SanctionDate,
      us_pa_dbo_Hist_SanctionTracking.ViolationDate,
      us_pa_dbo_Hist_SanctionTracking.LastModifiedBy,
      us_pa_dbo_Hist_SanctionTracking.LastModifiedDateTime,
      us_pa_dbo_Hist_SanctionTracking.OrgCode,
      us_pa_dbo_Hist_SanctionTracking.file_id,
      us_pa_dbo_Hist_SanctionTracking.is_deleted]
    sorts: [us_pa_dbo_Hist_SanctionTracking.ParoleNumber, us_pa_dbo_Hist_SanctionTracking.ParoleCountID, us_pa_dbo_Hist_SanctionTracking.SetID, us_pa_dbo_Hist_SanctionTracking.SequenceID, us_pa_dbo_Hist_SanctionTracking.SanctionCode]
    note_display: hover
    note_text: "A table describing violations of supervision conditions committed during specific periods of supervision, as well as the official responses to those violations."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 54
    col: 0
    width: 24
    height: 6

  - name: dbo_Hist_Treatment
    title: dbo_Hist_Treatment
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Hist_Treatment.primary_key,
      us_pa_dbo_Hist_Treatment.ParoleNumber,
      us_pa_dbo_Hist_Treatment.TrtHistoryID,
      us_pa_dbo_Hist_Treatment.TrtHistType,
      us_pa_dbo_Hist_Treatment.TrtHistStartDateYear,
      us_pa_dbo_Hist_Treatment.TrtHistStartDateMonth,
      us_pa_dbo_Hist_Treatment.TrtHistStartDateDay,
      us_pa_dbo_Hist_Treatment.TrtHistEndDateYear,
      us_pa_dbo_Hist_Treatment.TrtHistEndDateMonth,
      us_pa_dbo_Hist_Treatment.TrtHistEndDateDay,
      us_pa_dbo_Hist_Treatment.TrtHistProgramCode,
      us_pa_dbo_Hist_Treatment.TrtHistOutcomeStatusCode,
      us_pa_dbo_Hist_Treatment.TrtHistTreatmentDescription,
      us_pa_dbo_Hist_Treatment.TrtHistDOCO,
      us_pa_dbo_Hist_Treatment.ParoleCountID,
      us_pa_dbo_Hist_Treatment.OtherState,
      us_pa_dbo_Hist_Treatment.OtherStateFac,
      us_pa_dbo_Hist_Treatment.LastModifiedBy,
      us_pa_dbo_Hist_Treatment.LastModifiedDateTime,
      us_pa_dbo_Hist_Treatment.TrtHistHrsImposed,
      us_pa_dbo_Hist_Treatment.TrtHistHrsCompleted,
      us_pa_dbo_Hist_Treatment.file_id,
      us_pa_dbo_Hist_Treatment.is_deleted]
    sorts: [us_pa_dbo_Hist_Treatment.ParoleNumber, us_pa_dbo_Hist_Treatment.TrtHistoryID]
    note_display: hover
    note_text: "A table with information on referred treatments for people under supervision in the past. Rows are moved to this table from dbo_Treatment when a person exits supervision."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 60
    col: 0
    width: 24
    height: 6

  - name: dbo_LSIHistory
    title: dbo_LSIHistory
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_LSIHistory.primary_key,
      us_pa_dbo_LSIHistory.ParoleNumber,
      us_pa_dbo_LSIHistory.LsirID,
      us_pa_dbo_LSIHistory.ParoleCountID,
      us_pa_dbo_LSIHistory.ReleaseStatus,
      us_pa_dbo_LSIHistory.InstitutionNumber,
      us_pa_dbo_LSIHistory.RecordNumber,
      us_pa_dbo_LSIHistory.LSIType,
      us_pa_dbo_LSIHistory.QuikAdmin,
      us_pa_dbo_LSIHistory.AssessmentAge,
      us_pa_dbo_LSIHistory.AssessmentTime__raw,
      us_pa_dbo_LSIHistory.AssessmentGender,
      us_pa_dbo_LSIHistory.ClientAccount,
      us_pa_dbo_LSIHistory.Status,
      us_pa_dbo_LSIHistory.NumOmitQuestions,
      us_pa_dbo_LSIHistory.TotalScore,
      us_pa_dbo_LSIHistory.Percentile,
      us_pa_dbo_LSIHistory.ScoreCount,
      us_pa_dbo_LSIHistory.HistoryResults,
      us_pa_dbo_LSIHistory.HistoryRemark,
      us_pa_dbo_LSIHistory.OriginalClassification,
      us_pa_dbo_LSIHistory.OverridingClassification,
      us_pa_dbo_LSIHistory.SSNOrSIN,
      us_pa_dbo_LSIHistory.OtherID,
      us_pa_dbo_LSIHistory.ReferralSource,
      us_pa_dbo_LSIHistory.ReferralReason,
      us_pa_dbo_LSIHistory.Disposition,
      us_pa_dbo_LSIHistory.PresentOffenses,
      us_pa_dbo_LSIHistory.Rater,
      us_pa_dbo_LSIHistory.Code,
      us_pa_dbo_LSIHistory.PoliceFingerPrintNum,
      us_pa_dbo_LSIHistory.GradeLevel,
      us_pa_dbo_LSIHistory.MaritalStatus,
      us_pa_dbo_LSIHistory.OccupationalStanding,
      us_pa_dbo_LSIHistory.Employment,
      us_pa_dbo_LSIHistory.ReportPurpose,
      us_pa_dbo_LSIHistory.Context,
      us_pa_dbo_LSIHistory.HomelessOrTransient,
      us_pa_dbo_LSIHistory.HealthProblem,
      us_pa_dbo_LSIHistory.HealthSpecify,
      us_pa_dbo_LSIHistory.PhysicalDisability,
      us_pa_dbo_LSIHistory.PhysicalSpecify,
      us_pa_dbo_LSIHistory.Suicidal,
      us_pa_dbo_LSIHistory.SuicidalSpecify,
      us_pa_dbo_LSIHistory.LearningDisability,
      us_pa_dbo_LSIHistory.Immigration,
      us_pa_dbo_LSIHistory.IssueOther,
      us_pa_dbo_LSIHistory.IssueSpecify,
      us_pa_dbo_LSIHistory.ProbMinfrom,
      us_pa_dbo_LSIHistory.ProbMinto,
      us_pa_dbo_LSIHistory.ProbMedfrom,
      us_pa_dbo_LSIHistory.ProbMedto,
      us_pa_dbo_LSIHistory.ProbMax,
      us_pa_dbo_LSIHistory.HHPAFrom,
      us_pa_dbo_LSIHistory.HHPAto,
      us_pa_dbo_LSIHistory.HHPCloseSupfrom,
      us_pa_dbo_LSIHistory.HHPCloseSupto,
      us_pa_dbo_LSIHistory.HHPIntenseSup,
      us_pa_dbo_LSIHistory.IcMinfro,
      us_pa_dbo_LSIHistory.IcMinto,
      us_pa_dbo_LSIHistory.IcMedfrom,
      us_pa_dbo_LSIHistory.IcMedto,
      us_pa_dbo_LSIHistory.IcHighMedfrom,
      us_pa_dbo_LSIHistory.IcHighMedto,
      us_pa_dbo_LSIHistory.IcMax,
      us_pa_dbo_LSIHistory.AnsToQuestions,
      us_pa_dbo_LSIHistory.AnsQuest1Specify,
      us_pa_dbo_LSIHistory.AnsQuest4Specify,
      us_pa_dbo_LSIHistory.AnsQuest8Specify,
      us_pa_dbo_LSIHistory.AnsQuest40Specify,
      us_pa_dbo_LSIHistory.AnsQuest45Specify,
      us_pa_dbo_LSIHistory.AnsQuest50Specify,
      us_pa_dbo_LSIHistory.ObjectID,
      us_pa_dbo_LSIHistory.LastModifiedDateTime__raw,
      us_pa_dbo_LSIHistory.file_id,
      us_pa_dbo_LSIHistory.is_deleted]
    sorts: [us_pa_dbo_LSIHistory.AssessmentTime__raw]
    note_display: hover
    note_text: "A table containing LSIR assessments which have been conducted for people under supervision by PADOC."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 66
    col: 0
    width: 24
    height: 6

  - name: dbo_LSIR
    title: dbo_LSIR
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_LSIR.primary_key,
      us_pa_dbo_LSIR.ParoleNumber,
      us_pa_dbo_LSIR.ParoleCountID,
      us_pa_dbo_LSIR.LsirID,
      us_pa_dbo_LSIR.LSIRScore,
      us_pa_dbo_LSIR.InterviewDate__raw,
      us_pa_dbo_LSIR.file_id,
      us_pa_dbo_LSIR.is_deleted]
    sorts: [us_pa_dbo_LSIR.InterviewDate__raw]
    note_display: hover
    note_text: "A table containing LSIR assessments which have been conducted for people under supervision by PADOC. NOT REFRESHED REGULARLY."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 72
    col: 0
    width: 24
    height: 6

  - name: dbo_Miscon
    title: dbo_Miscon
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Miscon.primary_key,
      us_pa_dbo_Miscon.institution,
      us_pa_dbo_Miscon.misconduct_date__raw,
      us_pa_dbo_Miscon.form_141,
      us_pa_dbo_Miscon.data_followup,
      us_pa_dbo_Miscon.control_number,
      us_pa_dbo_Miscon.misconduct_number,
      us_pa_dbo_Miscon.sig_date,
      us_pa_dbo_Miscon.sig_time,
      us_pa_dbo_Miscon.user_id,
      us_pa_dbo_Miscon.inst_hvl_desc,
      us_pa_dbo_Miscon.report_date__raw,
      us_pa_dbo_Miscon.place_hvl_desc,
      us_pa_dbo_Miscon.place_hvl_code,
      us_pa_dbo_Miscon.place_extended,
      us_pa_dbo_Miscon.misconduct_time__raw,
      us_pa_dbo_Miscon.others_involved,
      us_pa_dbo_Miscon.ctgory_of_chrgs_1,
      us_pa_dbo_Miscon.ctgory_of_chrgs_2,
      us_pa_dbo_Miscon.ctgory_of_chrgs_3,
      us_pa_dbo_Miscon.ctgory_of_chrgs_4,
      us_pa_dbo_Miscon.ctgory_of_chrgs_5,
      us_pa_dbo_Miscon.confinement,
      us_pa_dbo_Miscon.confinement_date__raw,
      us_pa_dbo_Miscon.confinement_time,
      us_pa_dbo_Miscon.hearings_held,
      us_pa_dbo_Miscon.witnesses,
      us_pa_dbo_Miscon.inmate_version,
      us_pa_dbo_Miscon.recording_staff,
      us_pa_dbo_Miscon.rcrdng_staff_last,
      us_pa_dbo_Miscon.reviewing_staff,
      us_pa_dbo_Miscon.revwng_staff_last,
      us_pa_dbo_Miscon.date_reviewed__raw,
      us_pa_dbo_Miscon.inmate_notice_date__raw,
      us_pa_dbo_Miscon.inmate_notice_time,
      us_pa_dbo_Miscon.hearing_after_date__raw,
      us_pa_dbo_Miscon.hearing_after_time,
      us_pa_dbo_Miscon.status_141,
      us_pa_dbo_Miscon.stat_hvl_dsc_141,
      us_pa_dbo_Miscon.reason_802,
      us_pa_dbo_Miscon.rsn_hvl_desc_802,
      us_pa_dbo_Miscon.comment,
      us_pa_dbo_Miscon.drug_related,
      us_pa_dbo_Miscon.refer_formal_ind,
      us_pa_dbo_Miscon.file_id,
      us_pa_dbo_Miscon.is_deleted]
    sorts: [us_pa_dbo_Miscon.misconduct_date__raw]
    note_display: hover
    note_text: "A table describing conduct events that have taken place in a carceral setting involving an incarcerated person. This includes details about the event itself, as well as information about the outcome of that event, e.g. hearings held, reports drafted, and consequences handed down."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 78
    col: 0
    width: 24
    height: 6

  - name: dbo_Movrec
    title: dbo_Movrec
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Movrec.primary_key,
      us_pa_dbo_Movrec.mov_cnt_num,
      us_pa_dbo_Movrec.mov_seq_num,
      us_pa_dbo_Movrec.mov_chg_num,
      us_pa_dbo_Movrec.mov_cur_inmt_num,
      us_pa_dbo_Movrec.mov_sig_date__raw,
      us_pa_dbo_Movrec.mov_term_id,
      us_pa_dbo_Movrec.mov_sig_time,
      us_pa_dbo_Movrec.mov_user_id,
      us_pa_dbo_Movrec.mov_del_year,
      us_pa_dbo_Movrec.mov_del_month,
      us_pa_dbo_Movrec.mov_del_day,
      us_pa_dbo_Movrec.last_chg_num_used,
      us_pa_dbo_Movrec.mov_move_code,
      us_pa_dbo_Movrec.mov_move_date__raw,
      us_pa_dbo_Movrec.mov_move_time,
      us_pa_dbo_Movrec.mov_move_to_loc,
      us_pa_dbo_Movrec.parole_stat_cd,
      us_pa_dbo_Movrec.mov_sent_stat_cd,
      us_pa_dbo_Movrec.mov_rec_del_flag,
      us_pa_dbo_Movrec.mov_sent_group,
      us_pa_dbo_Movrec.mov_move_to_location_type,
      us_pa_dbo_Movrec.mov_move_from_location,
      us_pa_dbo_Movrec.mov_move_from_location_type,
      us_pa_dbo_Movrec.mov_permanent_institution,
      us_pa_dbo_Movrec.mov_to_institution,
      us_pa_dbo_Movrec.file_id,
      us_pa_dbo_Movrec.is_deleted]
    sorts: [us_pa_dbo_Movrec.mov_sig_date__raw]
    note_display: hover
    note_text: "A table describing physical movements of an incarcerated person from one carceral setting to another, e.g. transfers between prisons or temporary movements from prison to a hospital or court or releases from prison to supervision.  Rows in this table can be used to model \"edges\" of continuous periods of incarceration, and multiple such rows can be used to model a continuous period of incarceration."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 84
    col: 0
    width: 24
    height: 6

  - name: dbo_Offender
    title: dbo_Offender
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Offender.primary_key,
      us_pa_dbo_Offender.ParoleNumber,
      us_pa_dbo_Offender.PrlStatusCodeID,
      us_pa_dbo_Offender.OffSID,
      us_pa_dbo_Offender.OffReceptionDate__raw,
      us_pa_dbo_Offender.OffEducationGrade,
      us_pa_dbo_Offender.OffCitizenship,
      us_pa_dbo_Offender.OffAct14Notify,
      us_pa_dbo_Offender.OffHeight,
      us_pa_dbo_Offender.OffWeight,
      us_pa_dbo_Offender.OffFingerPrintCode,
      us_pa_dbo_Offender.OffSex,
      us_pa_dbo_Offender.OffCaseNumberAtInstitution,
      us_pa_dbo_Offender.OffReligionActive,
      us_pa_dbo_Offender.OffReligionOtherInfo,
      us_pa_dbo_Offender.OffCountyOfProgram,
      us_pa_dbo_Offender.OffRaceEthnicGroup,
      us_pa_dbo_Offender.OffEyeColor,
      us_pa_dbo_Offender.OffHairColor,
      us_pa_dbo_Offender.OffSkinTone,
      us_pa_dbo_Offender.OffMiscellaneous,
      us_pa_dbo_Offender.OffPreSentence,
      us_pa_dbo_Offender.OffStatusDateYear,
      us_pa_dbo_Offender.OffStatusDateMonth,
      us_pa_dbo_Offender.OffStatusDateDay,
      us_pa_dbo_Offender.OffRecordNumber,
      us_pa_dbo_Offender.OffSexOld,
      us_pa_dbo_Offender.OffRaceEthnicGroupOld,
      us_pa_dbo_Offender.EyeColor_ID,
      us_pa_dbo_Offender.HairColor_ID,
      us_pa_dbo_Offender.SkinTone_ID,
      us_pa_dbo_Offender.GangAffiliation_ID,
      us_pa_dbo_Offender.LastModifiedDate__raw,
      us_pa_dbo_Offender.LastModifiedBy,
      us_pa_dbo_Offender.file_id,
      us_pa_dbo_Offender.is_deleted]
    sorts: [us_pa_dbo_Offender.OffReceptionDate__raw]
    note_display: hover
    note_text: "A table containing demographic and identifier information for people who have been supervised by PADOC."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 90
    col: 0
    width: 24
    height: 6

  - name: dbo_ParoleCount
    title: dbo_ParoleCount
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_ParoleCount.primary_key,
      us_pa_dbo_ParoleCount.ParoleNumber,
      us_pa_dbo_ParoleCount.ParoleInstNumber,
      us_pa_dbo_ParoleCount.file_id,
      us_pa_dbo_ParoleCount.is_deleted]
    sorts: [us_pa_dbo_ParoleCount.ParoleNumber, us_pa_dbo_ParoleCount.ParoleInstNumber]
    note_display: hover
    note_text: "A table containing information that links Pennsylvania Board of Probation and Parole (PBPP) identification numbers to PA DOC inmate numbers."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 96
    col: 0
    width: 24
    height: 6

  - name: dbo_Parolee
    title: dbo_Parolee
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Parolee.primary_key,
      us_pa_dbo_Parolee.ParoleNumber,
      us_pa_dbo_Parolee.ptype_BoardParole,
      us_pa_dbo_Parolee.ptype_BoardReParole,
      us_pa_dbo_Parolee.Ptype_SpecProbParole,
      us_pa_dbo_Parolee.Ptype_OtherStateParole,
      us_pa_dbo_Parolee.Ptype_OtherStateProb,
      us_pa_dbo_Parolee.Pa_Drugs,
      us_pa_dbo_Parolee.PA_Alcoholic,
      us_pa_dbo_Parolee.PA_Psychiatric,
      us_pa_dbo_Parolee.PA_PastAssaultive,
      us_pa_dbo_Parolee.PA_Sexual,
      us_pa_dbo_Parolee.PA_Assaultive,
      us_pa_dbo_Parolee.PA_DomesticViolence,
      us_pa_dbo_Parolee.PA_Commutation,
      us_pa_dbo_Parolee.PA_Other,
      us_pa_dbo_Parolee.DOB,
      us_pa_dbo_Parolee.Notes,
      us_pa_dbo_Parolee.timestamp,
      us_pa_dbo_Parolee.LastName,
      us_pa_dbo_Parolee.FirstName,
      us_pa_dbo_Parolee.MiddleInitial,
      us_pa_dbo_Parolee.Suffix,
      us_pa_dbo_Parolee.Alias,
      us_pa_dbo_Parolee.SSAN,
      us_pa_dbo_Parolee.DriversLicenseNo,
      us_pa_dbo_Parolee.PictureId,
      us_pa_dbo_Parolee.CensusTract,
      us_pa_dbo_Parolee.AssignedAgentBadgeNum,
      us_pa_dbo_Parolee.SupervisorPosNo,
      us_pa_dbo_Parolee.Sex,
      us_pa_dbo_Parolee.Race,
      us_pa_dbo_Parolee.Height,
      us_pa_dbo_Parolee.Weight,
      us_pa_dbo_Parolee.Hair,
      us_pa_dbo_Parolee.Eyes,
      us_pa_dbo_Parolee.SkinTone,
      us_pa_dbo_Parolee.MaritalStatus,
      us_pa_dbo_Parolee.PA_OtherDesc,
      us_pa_dbo_Parolee.ParoleType,
      us_pa_dbo_Parolee.ParoleType2,
      us_pa_dbo_Parolee.dobid,
      us_pa_dbo_Parolee.SexOffenderCode,
      us_pa_dbo_Parolee.ParoleCountID,
      us_pa_dbo_Parolee.Miscellaneous,
      us_pa_dbo_Parolee.BoardParole,
      us_pa_dbo_Parolee.BoardReParole,
      us_pa_dbo_Parolee.SpecProbParole,
      us_pa_dbo_Parolee.OtherStateParole,
      us_pa_dbo_Parolee.OtherStateProb,
      us_pa_dbo_Parolee.Grade,
      us_pa_dbo_Parolee.GradeDate,
      us_pa_dbo_Parolee.Act97,
      us_pa_dbo_Parolee.file_id,
      us_pa_dbo_Parolee.is_deleted]
    sorts: [us_pa_dbo_Parolee.ParoleNumber]
    note_display: hover
    note_text: "A table containing information that for people who are on Supervision in Pennsylvania"
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 102
    col: 0
    width: 24
    height: 6

  - name: dbo_Perrec
    title: dbo_Perrec
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Perrec.primary_key,
      us_pa_dbo_Perrec.control_number,
      us_pa_dbo_Perrec.race,
      us_pa_dbo_Perrec.sex,
      us_pa_dbo_Perrec.ssn_1,
      us_pa_dbo_Perrec.date_of_birth,
      us_pa_dbo_Perrec.pitts_photo_num,
      us_pa_dbo_Perrec.ssn_2,
      us_pa_dbo_Perrec.commit_instit,
      us_pa_dbo_Perrec.boot_camp_elig,
      us_pa_dbo_Perrec.mandtry_sent_flag,
      us_pa_dbo_Perrec.ethnic_identity,
      us_pa_dbo_Perrec.marital_status,
      us_pa_dbo_Perrec.citizenship,
      us_pa_dbo_Perrec.religion,
      us_pa_dbo_Perrec.fbi_num,
      us_pa_dbo_Perrec.notify_name,
      us_pa_dbo_Perrec.notify_relatnship,
      us_pa_dbo_Perrec.notify_phone_num,
      us_pa_dbo_Perrec.notify_address_1,
      us_pa_dbo_Perrec.notify_address_2,
      us_pa_dbo_Perrec.notify_city,
      us_pa_dbo_Perrec.notify_state,
      us_pa_dbo_Perrec.notify_zip_code,
      us_pa_dbo_Perrec.legal_address_1,
      us_pa_dbo_Perrec.legal_address_2,
      us_pa_dbo_Perrec.legal_city,
      us_pa_dbo_Perrec.legal_state,
      us_pa_dbo_Perrec.legal_zip_code,
      us_pa_dbo_Perrec.city_of_birth,
      us_pa_dbo_Perrec.state_of_birth,
      us_pa_dbo_Perrec.country_of_birth,
      us_pa_dbo_Perrec.ssn_other,
      us_pa_dbo_Perrec.us_military_vet,
      us_pa_dbo_Perrec.vietnam_era_srvc,
      us_pa_dbo_Perrec.mil_from_year,
      us_pa_dbo_Perrec.mil_from_month,
      us_pa_dbo_Perrec.mil_to_yr,
      us_pa_dbo_Perrec.mil_to_mth,
      us_pa_dbo_Perrec.mil_dschrg_type,
      us_pa_dbo_Perrec.commit_time,
      us_pa_dbo_Perrec.commit_date__raw,
      us_pa_dbo_Perrec.commit_cnty,
      us_pa_dbo_Perrec.height_feet,
      us_pa_dbo_Perrec.height_inches,
      us_pa_dbo_Perrec.weight,
      us_pa_dbo_Perrec.eyes,
      us_pa_dbo_Perrec.hair,
      us_pa_dbo_Perrec.complexion,
      us_pa_dbo_Perrec.build,
      us_pa_dbo_Perrec.accomplices,
      us_pa_dbo_Perrec.affilatns_ind,
      us_pa_dbo_Perrec.affilatn_code_1,
      us_pa_dbo_Perrec.affilatn_code_2,
      us_pa_dbo_Perrec.affilatn_code_3,
      us_pa_dbo_Perrec.affilatn_code_4,
      us_pa_dbo_Perrec.affilatn_code_5,
      us_pa_dbo_Perrec.affilatn_code_6,
      us_pa_dbo_Perrec.affilatn_code_7,
      us_pa_dbo_Perrec.affilatn_code_8,
      us_pa_dbo_Perrec.affilatn_code_9,
      us_pa_dbo_Perrec.affilatn_code_10,
      us_pa_dbo_Perrec.pulhest_s,
      us_pa_dbo_Perrec.prob_alcohol,
      us_pa_dbo_Perrec.prob_drugs,
      us_pa_dbo_Perrec.prob_sexual,
      us_pa_dbo_Perrec.prob_assault,
      us_pa_dbo_Perrec.prob_escape,
      us_pa_dbo_Perrec.prob_suicide,
      us_pa_dbo_Perrec.prob_psyco,
      us_pa_dbo_Perrec.medical_limits,
      us_pa_dbo_Perrec.intellgnt_rate,
      us_pa_dbo_Perrec.grade_complete,
      us_pa_dbo_Perrec.wrat_date,
      us_pa_dbo_Perrec.wrat_test_r,
      us_pa_dbo_Perrec.wrat_test_s,
      us_pa_dbo_Perrec.wrat_test_a,
      us_pa_dbo_Perrec.total_score,
      us_pa_dbo_Perrec.program_level_1,
      us_pa_dbo_Perrec.program_level_2,
      us_pa_dbo_Perrec.program_level_3,
      us_pa_dbo_Perrec.ic_user_id,
      us_pa_dbo_Perrec.ic_data_entry_dt__raw,
      us_pa_dbo_Perrec.ic_lst_name,
      us_pa_dbo_Perrec.ic_fst_initial,
      us_pa_dbo_Perrec.ic_title,
      us_pa_dbo_Perrec.ic_date__raw,
      us_pa_dbo_Perrec.ic_reclass_date__raw,
      us_pa_dbo_Perrec.ic_recls_in_n_mths,
      us_pa_dbo_Perrec.ic_reclass_reason,
      us_pa_dbo_Perrec.ic_cur_off_cde_1,
      us_pa_dbo_Perrec.ic_cur_off_cde_2,
      us_pa_dbo_Perrec.ic_cur_off_cde_3,
      us_pa_dbo_Perrec.ic_prv_off_cde_1,
      us_pa_dbo_Perrec.ic_prv_off_cde_2,
      us_pa_dbo_Perrec.ic_prv_off_cde_3,
      us_pa_dbo_Perrec.ic_escpe_hist_1,
      us_pa_dbo_Perrec.ic_escpe_hist_2,
      us_pa_dbo_Perrec.ic_escpe_hist_3,
      us_pa_dbo_Perrec.ic_escpe_hist_4,
      us_pa_dbo_Perrec.ic_escpe_hist_5,
      us_pa_dbo_Perrec.ic_institut_adj,
      us_pa_dbo_Perrec.ic_prior_commits,
      us_pa_dbo_Perrec.ic_mths_to_release,
      us_pa_dbo_Perrec.ic_special_sent,
      us_pa_dbo_Perrec.ic_mrtl_stat_fr_cl,
      us_pa_dbo_Perrec.ic_employ_ind,
      us_pa_dbo_Perrec.ic_custdy_level,
      us_pa_dbo_Perrec.ic_ovride_cust_lvl,
      us_pa_dbo_Perrec.ic_prog_code_1,
      us_pa_dbo_Perrec.ic_prog_code_2,
      us_pa_dbo_Perrec.ic_prog_code_3,
      us_pa_dbo_Perrec.ic_medical_cond,
      us_pa_dbo_Perrec.ic_emotion_cond,
      us_pa_dbo_Perrec.ic_s_year,
      us_pa_dbo_Perrec.ic_s_month,
      us_pa_dbo_Perrec.ic_s_day,
      us_pa_dbo_Perrec.ic_da_no_abuse,
      us_pa_dbo_Perrec.ic_da_ed,
      us_pa_dbo_Perrec.ic_da_self_help,
      us_pa_dbo_Perrec.ic_da_ongoing,
      us_pa_dbo_Perrec.ic_da_therap,
      us_pa_dbo_Perrec.ic_alcohol,
      us_pa_dbo_Perrec.ic_drugs,
      us_pa_dbo_Perrec.ic_both_a_d,
      us_pa_dbo_Perrec.ic_da_self,
      us_pa_dbo_Perrec.ic_da_observa,
      us_pa_dbo_Perrec.ic_da_psi,
      us_pa_dbo_Perrec.ic_da_other,
      us_pa_dbo_Perrec.ic_da_score,
      us_pa_dbo_Perrec.ic_ed_cond,
      us_pa_dbo_Perrec.ic_ed_self_rpt,
      us_pa_dbo_Perrec.ic_ed_ed_rec,
      us_pa_dbo_Perrec.ic_ed_psi,
      us_pa_dbo_Perrec.ic_ed_other,
      us_pa_dbo_Perrec.ic_voc_cond,
      us_pa_dbo_Perrec.ic_voc_self_rpt,
      us_pa_dbo_Perrec.ic_voc_emp_rec,
      us_pa_dbo_Perrec.ic_voc_psi,
      us_pa_dbo_Perrec.ic_voc_other,
      us_pa_dbo_Perrec.ic_curr_off,
      us_pa_dbo_Perrec.ic_prev_off,
      us_pa_dbo_Perrec.ic_sex_none_known,
      us_pa_dbo_Perrec.ic_sex_minor,
      us_pa_dbo_Perrec.ic_sex_attempt,
      us_pa_dbo_Perrec.ic_sex_serus_force,
      us_pa_dbo_Perrec.ic_sex_serus_death,
      us_pa_dbo_Perrec.ic_othr_needs_cond,
      us_pa_dbo_Perrec.re_user_id,
      us_pa_dbo_Perrec.re_de_year,
      us_pa_dbo_Perrec.re_de_month,
      us_pa_dbo_Perrec.re_de_day,
      us_pa_dbo_Perrec.re_de_hour,
      us_pa_dbo_Perrec.re_de_minute,
      us_pa_dbo_Perrec.re_stf_l_name,
      us_pa_dbo_Perrec.re_stf_f_initial,
      us_pa_dbo_Perrec.re_title,
      us_pa_dbo_Perrec.recls_date__raw,
      us_pa_dbo_Perrec.re_recls_dte__raw,
      us_pa_dbo_Perrec.re_recls_in_n_mths,
      us_pa_dbo_Perrec.re_recls_reason,
      us_pa_dbo_Perrec.reclass_day,
      us_pa_dbo_Perrec.re_curr_off_cd_1,
      us_pa_dbo_Perrec.re_curr_off_cd_2,
      us_pa_dbo_Perrec.re_curr_off_cd_3,
      us_pa_dbo_Perrec.re_prev_off_cd_1,
      us_pa_dbo_Perrec.re_prev_off_cd_2,
      us_pa_dbo_Perrec.re_prev_off_cd_3,
      us_pa_dbo_Perrec.re_instit_violence,
      us_pa_dbo_Perrec.re_discip_reports,
      us_pa_dbo_Perrec.re_most_severe,
      us_pa_dbo_Perrec.re_age_for_class,
      us_pa_dbo_Perrec.re_escp_hist_1,
      us_pa_dbo_Perrec.re_escp_hist_2,
      us_pa_dbo_Perrec.re_escp_hist_3,
      us_pa_dbo_Perrec.re_escp_hist_4,
      us_pa_dbo_Perrec.re_escp_hist_5,
      us_pa_dbo_Perrec.re_prescript_prog,
      us_pa_dbo_Perrec.re_wrk_performnce,
      us_pa_dbo_Perrec.re_hous_perfrmnce,
      us_pa_dbo_Perrec.re_custody_level,
      us_pa_dbo_Perrec.re_ovride_cust_lvl,
      us_pa_dbo_Perrec.re_inmt_has_a_rcls,
      us_pa_dbo_Perrec.re_medical_cond,
      us_pa_dbo_Perrec.re_emotion_cond,
      us_pa_dbo_Perrec.re_s_rating_date__raw,
      us_pa_dbo_Perrec.re_da_no_abuse,
      us_pa_dbo_Perrec.re_da_ed,
      us_pa_dbo_Perrec.re_da_self_help,
      us_pa_dbo_Perrec.re_da_ongoing,
      us_pa_dbo_Perrec.re_da_therap,
      us_pa_dbo_Perrec.re_alcohol,
      us_pa_dbo_Perrec.re_drugs,
      us_pa_dbo_Perrec.re_both_a_d,
      us_pa_dbo_Perrec.re_da_self,
      us_pa_dbo_Perrec.re_da_observa,
      us_pa_dbo_Perrec.re_da_psi,
      us_pa_dbo_Perrec.re_da_other,
      us_pa_dbo_Perrec.re_da_score,
      us_pa_dbo_Perrec.re_ed_cond,
      us_pa_dbo_Perrec.re_ed_self_rpt,
      us_pa_dbo_Perrec.re_ed_ed_rec,
      us_pa_dbo_Perrec.re_ed_psi,
      us_pa_dbo_Perrec.re_ed_other,
      us_pa_dbo_Perrec.re_voc_cond,
      us_pa_dbo_Perrec.re_voc_self_rpt,
      us_pa_dbo_Perrec.re_voc_emp_rec,
      us_pa_dbo_Perrec.re_voc_psi,
      us_pa_dbo_Perrec.re_voc_other,
      us_pa_dbo_Perrec.re_curr_off,
      us_pa_dbo_Perrec.re_prev_off,
      us_pa_dbo_Perrec.re_sex_none_known,
      us_pa_dbo_Perrec.re_sex_minor,
      us_pa_dbo_Perrec.re_sex_attempt,
      us_pa_dbo_Perrec.re_sex_ser_force,
      us_pa_dbo_Perrec.re_sex_ser_death,
      us_pa_dbo_Perrec.re_othr_needs_cond,
      us_pa_dbo_Perrec.pulhest_user_id,
      us_pa_dbo_Perrec.pulhest_entry_date,
      us_pa_dbo_Perrec.phila_photo_num,
      us_pa_dbo_Perrec.inmate_num,
      us_pa_dbo_Perrec.custody,
      us_pa_dbo_Perrec.conf_ind,
      us_pa_dbo_Perrec.temp_custody,
      us_pa_dbo_Perrec.temp_p_code_1,
      us_pa_dbo_Perrec.temp_p_code_2,
      us_pa_dbo_Perrec.temp_p_code_3,
      us_pa_dbo_Perrec.ovride_lock,
      us_pa_dbo_Perrec.out_status_flag,
      us_pa_dbo_Perrec.sig_date__raw,
      us_pa_dbo_Perrec.sig_time,
      us_pa_dbo_Perrec.user_id,
      us_pa_dbo_Perrec.ovride_pending,
      us_pa_dbo_Perrec.confidential_date,
      us_pa_dbo_Perrec.problematic_offenses,
      us_pa_dbo_Perrec.parole_board_num,
      us_pa_dbo_Perrec.test_type,
      us_pa_dbo_Perrec.file_id,
      us_pa_dbo_Perrec.is_deleted]
    sorts: [us_pa_dbo_Perrec.commit_date__raw]
    note_display: hover
    note_text: "A table containing demographic and identifier information for people who have been incarcerated by PADOC.  In addition to basic demographics and identifiers, this table also acts as a \"roll-up\" table for a variety of bits of information contained in other PADOC tables, in order to serve as a quick interface for understanding the current state of the person.  A large majority of the fields in this table are unused by Recidiviz, particularly those which delve into highly sensitive information about a person."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 108
    col: 0
    width: 24
    height: 6

  - name: dbo_RelAgentHistory
    title: dbo_RelAgentHistory
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_RelAgentHistory.primary_key,
      us_pa_dbo_RelAgentHistory.ParoleNumber,
      us_pa_dbo_RelAgentHistory.ParoleCountID,
      us_pa_dbo_RelAgentHistory.AgentName,
      us_pa_dbo_RelAgentHistory.SupervisorName,
      us_pa_dbo_RelAgentHistory.LastModifiedBy,
      us_pa_dbo_RelAgentHistory.LastModifiedDateTime__raw,
      us_pa_dbo_RelAgentHistory.Agent_EmpNum,
      us_pa_dbo_RelAgentHistory.Supervisor_EmpNum,
      us_pa_dbo_RelAgentHistory.file_id,
      us_pa_dbo_RelAgentHistory.is_deleted]
    sorts: [us_pa_dbo_RelAgentHistory.LastModifiedDateTime__raw]
    note_display: hover
    note_text: "A table describing transfers between supervising agents for specific periods of supervision presently or previously served under the authority of PADOC. Release in this context refers to the release of a person from incarceration to supervision, though a minority of these rows include, for example, stints of probation being served in lieu of incarceration.  This table is typically joined with several other \"Release\" related tables that round out the picture of a period of supervision."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 114
    col: 0
    width: 24
    height: 6

  - name: dbo_RelEmployment
    title: dbo_RelEmployment
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_RelEmployment.primary_key,
      us_pa_dbo_RelEmployment.ParoleNumber,
      us_pa_dbo_RelEmployment.ParoleCountID,
      us_pa_dbo_RelEmployment.RelEmploymentStatus,
      us_pa_dbo_RelEmployment.RelEmpStatusDateYear,
      us_pa_dbo_RelEmployment.RelEmpStatusDateMonth,
      us_pa_dbo_RelEmployment.RelEmpStatusDateDay,
      us_pa_dbo_RelEmployment.file_id,
      us_pa_dbo_RelEmployment.is_deleted]
    sorts: [us_pa_dbo_RelEmployment.ParoleNumber, us_pa_dbo_RelEmployment.ParoleCountID]
    note_display: hover
    note_text: "A table describing specific periods of supervision currently being served by a particular person under the authority of PADOC. Release in this context refers to the release of a person from incarceration to supervision, though a minority of these rows include, for example, stints of probation being served in lieu of incarceration.  This includes specifically the current employment status of person during this period and when the employment status came into effect. This table is typically joined with several other \"Release\" related tables that round out the picture of a period of supervision.  NOT REFRESHED REGULARLY."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 120
    col: 0
    width: 24
    height: 6

  - name: dbo_Release
    title: dbo_Release
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Release.primary_key,
      us_pa_dbo_Release.ParoleNumber,
      us_pa_dbo_Release.ParoleCountID,
      us_pa_dbo_Release.RelEntryCodeOfCase,
      us_pa_dbo_Release.RelEntryCodeOfCase2,
      us_pa_dbo_Release.RelAcceptanceDateYear,
      us_pa_dbo_Release.RelAcceptanceDateMonth,
      us_pa_dbo_Release.RelAcceptanceDateDay,
      us_pa_dbo_Release.RelReleaseDateYear,
      us_pa_dbo_Release.RelReleaseDateMonth,
      us_pa_dbo_Release.RelReleaseDateDay,
      us_pa_dbo_Release.ReleaseProcessingDateYear,
      us_pa_dbo_Release.ReleaseProcessingDateMonth,
      us_pa_dbo_Release.ReleaseProcessingDateDay,
      us_pa_dbo_Release.RelEntryProcessingDateYear,
      us_pa_dbo_Release.RelEntryProcessingDateMonth,
      us_pa_dbo_Release.RelEntryProcessingDateDay,
      us_pa_dbo_Release.LastModifiedDate__raw,
      us_pa_dbo_Release.LastModifiedBy,
      us_pa_dbo_Release.file_id,
      us_pa_dbo_Release.is_deleted]
    sorts: [us_pa_dbo_Release.LastModifiedDate__raw]
    note_display: hover
    note_text: "A table describing specific periods of supervision currently being served by a particular person under the authority of PADOC. Release in this context refers to the release of a person from incarceration to supervision, though a minority of these rows include, for example, stints of probation being served in lieu of incarceration.  This includes when the period of supervision began and the type of supervision is being served. This table is typically joined with several other \"Release\" related tables that round out the picture of a period of supervision."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 126
    col: 0
    width: 24
    height: 6

  - name: dbo_ReleaseInfo
    title: dbo_ReleaseInfo
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_ReleaseInfo.primary_key,
      us_pa_dbo_ReleaseInfo.ParoleNumber,
      us_pa_dbo_ReleaseInfo.ParoleCountID,
      us_pa_dbo_ReleaseInfo.RelCMCCode,
      us_pa_dbo_ReleaseInfo.ReleaseFrom,
      us_pa_dbo_ReleaseInfo.RelCountyResidence,
      us_pa_dbo_ReleaseInfo.RelDO,
      us_pa_dbo_ReleaseInfo.RelPBPPOffenseCode,
      us_pa_dbo_ReleaseInfo.WangAgentName,
      us_pa_dbo_ReleaseInfo.RelPurgeCode,
      us_pa_dbo_ReleaseInfo.RelOverRideIndicator,
      us_pa_dbo_ReleaseInfo.RelWantIndicator,
      us_pa_dbo_ReleaseInfo.RelTransferDateYear,
      us_pa_dbo_ReleaseInfo.RelTransferDateMonth,
      us_pa_dbo_ReleaseInfo.RelTransferDateDay,
      us_pa_dbo_ReleaseInfo.RelMaxDate__raw,
      us_pa_dbo_ReleaseInfo.WangClientName,
      us_pa_dbo_ReleaseInfo.RelPBPPOffenseCode2,
      us_pa_dbo_ReleaseInfo.RelPBPPOffenseCode3,
      us_pa_dbo_ReleaseInfo.RelDOOld,
      us_pa_dbo_ReleaseInfo.RelDONew,
      us_pa_dbo_ReleaseInfo.RelOTN1,
      us_pa_dbo_ReleaseInfo.RelOTN2,
      us_pa_dbo_ReleaseInfo.RelOTN3,
      us_pa_dbo_ReleaseInfo.RelMaxDate1,
      us_pa_dbo_ReleaseInfo.RelMaxDate2,
      us_pa_dbo_ReleaseInfo.RelMaxDate3,
      us_pa_dbo_ReleaseInfo.RelMinDate1,
      us_pa_dbo_ReleaseInfo.RelMinDate2,
      us_pa_dbo_ReleaseInfo.RelMinDate3,
      us_pa_dbo_ReleaseInfo.relCurrentRiskGrade,
      us_pa_dbo_ReleaseInfo.relCurrentRiskGradeOverRide,
      us_pa_dbo_ReleaseInfo.relCurrentRiskGradeDateYear,
      us_pa_dbo_ReleaseInfo.relCurrentRiskGradeDateMonth,
      us_pa_dbo_ReleaseInfo.relCurrentRiskGradeDateDay,
      us_pa_dbo_ReleaseInfo.MaxAssessID,
      us_pa_dbo_ReleaseInfo.temp_RelDO,
      us_pa_dbo_ReleaseInfo.temp_WangAgentName,
      us_pa_dbo_ReleaseInfo.temp_County,
      us_pa_dbo_ReleaseInfo.temp_RelSupervisor,
      us_pa_dbo_ReleaseInfo.temp_OverRideGrade2,
      us_pa_dbo_ReleaseInfo.RelFinalRiskGrade,
      us_pa_dbo_ReleaseInfo.Supervisor,
      us_pa_dbo_ReleaseInfo.relWeight,
      us_pa_dbo_ReleaseInfo.OrgCode,
      us_pa_dbo_ReleaseInfo.RelLUOffenseID,
      us_pa_dbo_ReleaseInfo.RelLUOffenseID2,
      us_pa_dbo_ReleaseInfo.RelLUOffenseID3,
      us_pa_dbo_ReleaseInfo.tempWangAgentName,
      us_pa_dbo_ReleaseInfo.RskAct97Positive,
      us_pa_dbo_ReleaseInfo.RskAct97ViolationOfSbstanceAct,
      us_pa_dbo_ReleaseInfo.RskAct97DrugRelatedOffense,
      us_pa_dbo_ReleaseInfo.RskUrinalysis,
      us_pa_dbo_ReleaseInfo.RskBoardMandatedUrinala,
      us_pa_dbo_ReleaseInfo.Rsk03NmbrOfPriorFelonyCnvcts,
      us_pa_dbo_ReleaseInfo.RskAct97Other,
      us_pa_dbo_ReleaseInfo.file_id,
      us_pa_dbo_ReleaseInfo.is_deleted]
    sorts: [us_pa_dbo_ReleaseInfo.RelMaxDate__raw]
    note_display: hover
    note_text: "A table describing specific periods of supervision currently being served by a particular person under the authority of PADOC. Release in this context refers to the release of a person from incarceration to supervision, though a minority of these rows include, for example, stints of probation being served in lieu of incarceration.  This includes a wide variety of kinds of metadata, including the supervision officer and field office, the offenses that this period of supervision was sentenced for, and more. This table is typically joined with several other \"Release\" related tables that round out the picture of a period of supervision."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 132
    col: 0
    width: 24
    height: 6

  - name: dbo_SanctionTracking
    title: dbo_SanctionTracking
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_SanctionTracking.primary_key,
      us_pa_dbo_SanctionTracking.ParoleNumber,
      us_pa_dbo_SanctionTracking.ParoleCountID,
      us_pa_dbo_SanctionTracking.SetID,
      us_pa_dbo_SanctionTracking.SequenceID,
      us_pa_dbo_SanctionTracking.Type,
      us_pa_dbo_SanctionTracking.SanctionCode,
      us_pa_dbo_SanctionTracking.SanctionDate,
      us_pa_dbo_SanctionTracking.ViolationDate,
      us_pa_dbo_SanctionTracking.LastModifiedBy,
      us_pa_dbo_SanctionTracking.LastModifiedDateTime,
      us_pa_dbo_SanctionTracking.OrgCode,
      us_pa_dbo_SanctionTracking.file_id,
      us_pa_dbo_SanctionTracking.is_deleted]
    sorts: [us_pa_dbo_SanctionTracking.ParoleNumber, us_pa_dbo_SanctionTracking.ParoleCountID, us_pa_dbo_SanctionTracking.SetID, us_pa_dbo_SanctionTracking.SequenceID]
    note_display: hover
    note_text: "A table describing violations of supervision conditions committed during specific periods of supervision, as well as the official responses to those violations."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 138
    col: 0
    width: 24
    height: 6

  - name: dbo_Senrec
    title: dbo_Senrec
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Senrec.primary_key,
      us_pa_dbo_Senrec.curr_inmate_num,
      us_pa_dbo_Senrec.type_number,
      us_pa_dbo_Senrec.addit_sent_detnr,
      us_pa_dbo_Senrec.bail_yrs,
      us_pa_dbo_Senrec.bail_mths,
      us_pa_dbo_Senrec.bail_days,
      us_pa_dbo_Senrec.class_of_sent,
      us_pa_dbo_Senrec.commit_crdit_yrs,
      us_pa_dbo_Senrec.commit_crdit_mths,
      us_pa_dbo_Senrec.commit_crdit_days,
      us_pa_dbo_Senrec.max_cort_sent_yrs,
      us_pa_dbo_Senrec.max_cort_sent_mths,
      us_pa_dbo_Senrec.max_cort_sent_days,
      us_pa_dbo_Senrec.max_cort_sent_l_da,
      us_pa_dbo_Senrec.min_cort_sent_yrs,
      us_pa_dbo_Senrec.min_cort_sent_mths,
      us_pa_dbo_Senrec.min_cort_sent_days,
      us_pa_dbo_Senrec.min_cort_sent_l_da,
      us_pa_dbo_Senrec.effective_date,
      us_pa_dbo_Senrec.escape_yrs,
      us_pa_dbo_Senrec.escape_mths,
      us_pa_dbo_Senrec.escape_days,
      us_pa_dbo_Senrec.max_expir_date__raw,
      us_pa_dbo_Senrec.min_expir_date,
      us_pa_dbo_Senrec.max_fac_sent_yrs,
      us_pa_dbo_Senrec.max_fac_sent_mths,
      us_pa_dbo_Senrec.max_fac_sent_days,
      us_pa_dbo_Senrec.min_fac_sent_yrs,
      us_pa_dbo_Senrec.min_fac_sent_mths,
      us_pa_dbo_Senrec.min_fac_sent_days,
      us_pa_dbo_Senrec.gbmi,
      us_pa_dbo_Senrec.indictment_num,
      us_pa_dbo_Senrec.judge,
      us_pa_dbo_Senrec.offense_code,
      us_pa_dbo_Senrec.offense_track_num,
      us_pa_dbo_Senrec.parole_status_cde,
      us_pa_dbo_Senrec.parole_status_dt__raw,
      us_pa_dbo_Senrec.sent_date__raw,
      us_pa_dbo_Senrec.sent_start_date__raw,
      us_pa_dbo_Senrec.sent_status_code,
      us_pa_dbo_Senrec.sent_status_date__raw,
      us_pa_dbo_Senrec.sent_stop_date__raw,
      us_pa_dbo_Senrec.sentcing_cnty,
      us_pa_dbo_Senrec.st_to_frm_compact,
      us_pa_dbo_Senrec.term_of_cort,
      us_pa_dbo_Senrec.type_of_sent,
      us_pa_dbo_Senrec.crime_facts_ind,
      us_pa_dbo_Senrec.megans_law_ind,
      us_pa_dbo_Senrec.sig_date__raw,
      us_pa_dbo_Senrec.sig_time,
      us_pa_dbo_Senrec.user_id,
      us_pa_dbo_Senrec.cntinued_frm_doc_n,
      us_pa_dbo_Senrec.file_id,
      us_pa_dbo_Senrec.is_deleted]
    sorts: [us_pa_dbo_Senrec.max_expir_date__raw]
    note_display: hover
    note_text: "A table containing sentences to incarceration to be served under the authority of PADOC. This includes information about the terms of the sentence, as well as a small amount of information about the offenses and court case that led to the sentence."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 144
    col: 0
    width: 24
    height: 6

  - name: dbo_Sentence
    title: dbo_Sentence
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Sentence.primary_key,
      us_pa_dbo_Sentence.ParoleNumber,
      us_pa_dbo_Sentence.ParoleCountID,
      us_pa_dbo_Sentence.Sent16DGroupNumber,
      us_pa_dbo_Sentence.SentenceID,
      us_pa_dbo_Sentence.SentMonth,
      us_pa_dbo_Sentence.SentDay,
      us_pa_dbo_Sentence.SentYear,
      us_pa_dbo_Sentence.SentTerm,
      us_pa_dbo_Sentence.SentType,
      us_pa_dbo_Sentence.SentOffense,
      us_pa_dbo_Sentence.SentOTN,
      us_pa_dbo_Sentence.SentMinSentenceYear,
      us_pa_dbo_Sentence.SentMinSentenceMonth,
      us_pa_dbo_Sentence.SentMinSentenceDay,
      us_pa_dbo_Sentence.SentMaxSentenceYear,
      us_pa_dbo_Sentence.SentMaxSentenceMonth,
      us_pa_dbo_Sentence.SentMaxSentenceDay,
      us_pa_dbo_Sentence.SentCounty,
      us_pa_dbo_Sentence.SentOffense2,
      us_pa_dbo_Sentence.SentOffense3,
      us_pa_dbo_Sentence.SentCentury,
      us_pa_dbo_Sentence.sentCodeSentOffense,
      us_pa_dbo_Sentence.sentCodeSentOffense2,
      us_pa_dbo_Sentence.sentCodeSentOffense3,
      us_pa_dbo_Sentence.Display,
      us_pa_dbo_Sentence.file_id,
      us_pa_dbo_Sentence.is_deleted]
    sorts: [us_pa_dbo_Sentence.ParoleNumber, us_pa_dbo_Sentence.ParoleCountID, us_pa_dbo_Sentence.Sent16DGroupNumber, us_pa_dbo_Sentence.SentenceID]
    note_display: hover
    note_text: "A table containing sentences to incarceration or supervision to be served under the authority of PADOC. Because probation is administered almost entirely at the county level in Pennsylvania and PADOC/PBPP primarily deals with parole, the majority of sentences in this table are sentences to incarceration that in turn led to releases to parole. Consequently, much of this information is redundant of the information directly from the DOC side of PADOC via `dbo_Senrec` and may not be as reliable/consistent."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 150
    col: 0
    width: 24
    height: 6

  - name: dbo_SentenceGroup
    title: dbo_SentenceGroup
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_SentenceGroup.primary_key,
      us_pa_dbo_SentenceGroup.ParoleNumber,
      us_pa_dbo_SentenceGroup.ParoleCountID,
      us_pa_dbo_SentenceGroup.Sent16DGroupNumber,
      us_pa_dbo_SentenceGroup.SenProbInd,
      us_pa_dbo_SentenceGroup.SenTotMinYear,
      us_pa_dbo_SentenceGroup.SenTotMinMonth,
      us_pa_dbo_SentenceGroup.SenTotMinDay,
      us_pa_dbo_SentenceGroup.SenTotMaxYear,
      us_pa_dbo_SentenceGroup.SenTotMaxMonth,
      us_pa_dbo_SentenceGroup.SenTotMaxDay,
      us_pa_dbo_SentenceGroup.SenMinMonth,
      us_pa_dbo_SentenceGroup.SenMinDay,
      us_pa_dbo_SentenceGroup.SenMinYear,
      us_pa_dbo_SentenceGroup.SenMaxMonth,
      us_pa_dbo_SentenceGroup.SenMaxDay,
      us_pa_dbo_SentenceGroup.SenMaxYear,
      us_pa_dbo_SentenceGroup.SenLetter,
      us_pa_dbo_SentenceGroup.SenAmisc,
      us_pa_dbo_SentenceGroup.SenAddDate__raw,
      us_pa_dbo_SentenceGroup.SenLastModDate__raw,
      us_pa_dbo_SentenceGroup.SentEffectiveDate__raw,
      us_pa_dbo_SentenceGroup.WANGDataFlag,
      us_pa_dbo_SentenceGroup.SenOffLastName,
      us_pa_dbo_SentenceGroup.SenOffFirstName,
      us_pa_dbo_SentenceGroup.SenOffMidName,
      us_pa_dbo_SentenceGroup.SenOffSuffix,
      us_pa_dbo_SentenceGroup.SenInstitution,
      us_pa_dbo_SentenceGroup.SenInstNumber,
      us_pa_dbo_SentenceGroup.SenAdjMaxMonth,
      us_pa_dbo_SentenceGroup.SenAdjMaxDay,
      us_pa_dbo_SentenceGroup.SenAdjMaxYear,
      us_pa_dbo_SentenceGroup.senRRRICaseInd,
      us_pa_dbo_SentenceGroup.senRRRIDate__raw,
      us_pa_dbo_SentenceGroup.senRebuttCaseInd,
      us_pa_dbo_SentenceGroup.senTrueMinDate__raw,
      us_pa_dbo_SentenceGroup.file_id,
      us_pa_dbo_SentenceGroup.is_deleted]
    sorts: [us_pa_dbo_SentenceGroup.SenAddDate__raw]
    note_display: hover
    note_text: "A table containing \"roll-up\" information about all sentences in a group of sentences to incarceration or supervision to be served under the authority of PADOC. Because probation is administered almost entirely at the county level in Pennsylvania and PADOC/PBPP primarily deals with parole, the majority of sentences in this table are sentences to incarceration that in turn led to releases to parole. Consequently, much of this information is redundant of the information directly from the DOC side of PADOC via `dbo_Senrec` and may not be as reliable/consistent."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 156
    col: 0
    width: 24
    height: 6

  - name: dbo_Treatment
    title: dbo_Treatment
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_Treatment.primary_key,
      us_pa_dbo_Treatment.ParoleNumber,
      us_pa_dbo_Treatment.ParoleCountID,
      us_pa_dbo_Treatment.TreatmentID,
      us_pa_dbo_Treatment.TrtType,
      us_pa_dbo_Treatment.TrtDO,
      us_pa_dbo_Treatment.TrtCounty,
      us_pa_dbo_Treatment.TrtClassCode,
      us_pa_dbo_Treatment.TrtIndProgID,
      us_pa_dbo_Treatment.TrtStartDateYear,
      us_pa_dbo_Treatment.TrtStartDateMonth,
      us_pa_dbo_Treatment.TrtStartDateDay,
      us_pa_dbo_Treatment.TrtEndDateYear,
      us_pa_dbo_Treatment.TrtEndDateMonth,
      us_pa_dbo_Treatment.TrtEndDateDay,
      us_pa_dbo_Treatment.TrtStatusCode,
      us_pa_dbo_Treatment.TrtProgramCode,
      us_pa_dbo_Treatment.TrtProgramDescription,
      us_pa_dbo_Treatment.TrtIndexAct97,
      us_pa_dbo_Treatment.OtherState,
      us_pa_dbo_Treatment.OtherStateFac,
      us_pa_dbo_Treatment.LastModifiedBy,
      us_pa_dbo_Treatment.LastModifiedDateTime__raw,
      us_pa_dbo_Treatment.TrtHrsImposed,
      us_pa_dbo_Treatment.TrtHrsCompleted,
      us_pa_dbo_Treatment.file_id,
      us_pa_dbo_Treatment.is_deleted]
    sorts: [us_pa_dbo_Treatment.LastModifiedDateTime__raw]
    note_display: hover
    note_text: "A table with information on referred treatments for people under supervision."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 162
    col: 0
    width: 24
    height: 6

  - name: dbo_tblInmTestScore
    title: dbo_tblInmTestScore
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_tblInmTestScore.primary_key,
      us_pa_dbo_tblInmTestScore.Test_Id,
      us_pa_dbo_tblInmTestScore.Control_Number,
      us_pa_dbo_tblInmTestScore.Test_Desc,
      us_pa_dbo_tblInmTestScore.Inmate_number,
      us_pa_dbo_tblInmTestScore.Test_Dt__raw,
      us_pa_dbo_tblInmTestScore.Fac_Cd,
      us_pa_dbo_tblInmTestScore.Test_Score,
      us_pa_dbo_tblInmTestScore.ModBy_EmpNum,
      us_pa_dbo_tblInmTestScore.LstMod_Dt__raw,
      us_pa_dbo_tblInmTestScore.AsmtVer_Num,
      us_pa_dbo_tblInmTestScore.Fab_ind,
      us_pa_dbo_tblInmTestScore.RSTRvsd_Flg,
      us_pa_dbo_tblInmTestScore.file_id,
      us_pa_dbo_tblInmTestScore.is_deleted]
    sorts: [us_pa_dbo_tblInmTestScore.Test_Dt__raw]
    note_display: hover
    note_text: "A table containing assessments administered to people incarcerated under the authority of PADOC. This version of the table specifically contains assessments for people who are currently incarcerated."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 168
    col: 0
    width: 24
    height: 6

  - name: dbo_tblInmTestScoreHist
    title: dbo_tblInmTestScoreHist
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_tblInmTestScoreHist.primary_key,
      us_pa_dbo_tblInmTestScoreHist.Test_Id,
      us_pa_dbo_tblInmTestScoreHist.Control_Number,
      us_pa_dbo_tblInmTestScoreHist.Hist_dt__raw,
      us_pa_dbo_tblInmTestScoreHist.AsmtVer_Num,
      us_pa_dbo_tblInmTestScoreHist.Test_Desc,
      us_pa_dbo_tblInmTestScoreHist.Inmate_number,
      us_pa_dbo_tblInmTestScoreHist.Test_Dt__raw,
      us_pa_dbo_tblInmTestScoreHist.Fac_Cd,
      us_pa_dbo_tblInmTestScoreHist.Test_Score,
      us_pa_dbo_tblInmTestScoreHist.ModBy_EmpNum,
      us_pa_dbo_tblInmTestScoreHist.LstMod_Dt__raw,
      us_pa_dbo_tblInmTestScoreHist.RSTRvsd_Flg,
      us_pa_dbo_tblInmTestScoreHist.FAB_ind,
      us_pa_dbo_tblInmTestScoreHist.file_id,
      us_pa_dbo_tblInmTestScoreHist.is_deleted]
    sorts: [us_pa_dbo_tblInmTestScoreHist.Hist_dt__raw]
    note_display: hover
    note_text: "A table containing assessments administered to people incarcerated under the authority of PADOC. This version of the table specifically contains assessments for people who were previously incarcerated."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 174
    col: 0
    width: 24
    height: 6

  - name: dbo_vwCCISAllMvmt
    title: dbo_vwCCISAllMvmt
    explore: us_pa_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_pa_dbo_vwCCISAllMvmt.primary_key,
      us_pa_dbo_vwCCISAllMvmt.CCISMvmt_ID,
      us_pa_dbo_vwCCISAllMvmt.Mvmt_SeqNum,
      us_pa_dbo_vwCCISAllMvmt.Case_id,
      us_pa_dbo_vwCCISAllMvmt.Inmate_Number,
      us_pa_dbo_vwCCISAllMvmt.Status_Id,
      us_pa_dbo_vwCCISAllMvmt.Status_Dt__raw,
      us_pa_dbo_vwCCISAllMvmt.Status_Tm,
      us_pa_dbo_vwCCISAllMvmt.Move_Cd,
      us_pa_dbo_vwCCISAllMvmt.SentStatus_Cd,
      us_pa_dbo_vwCCISAllMvmt.ParoleStatus_Cd,
      us_pa_dbo_vwCCISAllMvmt.RegionFrom,
      us_pa_dbo_vwCCISAllMvmt.LocationFrom_Cd,
      us_pa_dbo_vwCCISAllMvmt.LocationTo_Cd,
      us_pa_dbo_vwCCISAllMvmt.CommentTypeId,
      us_pa_dbo_vwCCISAllMvmt.CommentType_Desc,
      us_pa_dbo_vwCCISAllMvmt.Comments,
      us_pa_dbo_vwCCISAllMvmt.LstMod_Dt__raw,
      us_pa_dbo_vwCCISAllMvmt.LstMod_EmpNum,
      us_pa_dbo_vwCCISAllMvmt.Bed_Dt__raw,
      us_pa_dbo_vwCCISAllMvmt.Cnslr_EmpNum,
      us_pa_dbo_vwCCISAllMvmt.file_id,
      us_pa_dbo_vwCCISAllMvmt.is_deleted]
    sorts: [us_pa_dbo_vwCCISAllMvmt.Status_Dt__raw]
    note_display: hover
    note_text: "A table from the Community Corrections Information System (CCIS) tracking the movements of individuals in Community Corrections Centers and Community Contract Facilities."
    listen: 
      View Type: us_pa_dbo_tblSearchInmateInfo.view_type
      US_PA_INMATE: us_pa_dbo_tblSearchInmateInfo.inmate_number
      US_PA_CONT: us_pa_dbo_tblSearchInmateInfo.control_number
      US_PA_PBPP: us_pa_dbo_tblSearchInmateInfo.parole_board_num
    row: 180
    col: 0
    width: 24
    height: 6

