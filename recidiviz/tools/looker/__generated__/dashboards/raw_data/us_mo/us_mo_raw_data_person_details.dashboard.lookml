# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_mo_raw_data_person_details
  title: Missouri Raw Data Person Details
  description: For examining individuals in US_MO's raw data tables
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
    explore: us_mo_raw_data
    field: us_mo_LBAKRDTA_TAK001.view_type

  - name: US_MO_DOC
    title: US_MO_DOC
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mo_raw_data
    field: us_mo_LBAKRDTA_TAK001.EK_DOC

  - name: US_MO_SID
    title: US_MO_SID
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mo_raw_data
    field: us_mo_LBAKRDTA_TAK001.EK_SID

  - name: US_MO_FBI
    title: US_MO_FBI
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mo_raw_data
    field: us_mo_LBAKRDTA_TAK001.EK_FBI

  - name: US_MO_OLN
    title: US_MO_OLN
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{model_name}"
    explore: us_mo_raw_data
    field: us_mo_LBAKRDTA_TAK001.EK_OLN

  elements:
  - name: LBAKRDTA_TAK001
    title: LBAKRDTA_TAK001
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK001.primary_key,
      us_mo_LBAKRDTA_TAK001.EK_DOC,
      us_mo_LBAKRDTA_TAK001.EK_CYC,
      us_mo_LBAKRDTA_TAK001.EK_ALN,
      us_mo_LBAKRDTA_TAK001.EK_AFN,
      us_mo_LBAKRDTA_TAK001.EK_AMI,
      us_mo_LBAKRDTA_TAK001.EK_AGS,
      us_mo_LBAKRDTA_TAK001.EK_NRN,
      us_mo_LBAKRDTA_TAK001.EK_SID,
      us_mo_LBAKRDTA_TAK001.EK_FBI,
      us_mo_LBAKRDTA_TAK001.EK_OLN,
      us_mo_LBAKRDTA_TAK001.EK_OLC,
      us_mo_LBAKRDTA_TAK001.EK_FOI,
      us_mo_LBAKRDTA_TAK001.EK_PLC,
      us_mo_LBAKRDTA_TAK001.EK_FLC,
      us_mo_LBAKRDTA_TAK001.EK_OLA,
      us_mo_LBAKRDTA_TAK001.EK_PLA,
      us_mo_LBAKRDTA_TAK001.EK_FLA,
      us_mo_LBAKRDTA_TAK001.EK_AV,
      us_mo_LBAKRDTA_TAK001.EK_LE,
      us_mo_LBAKRDTA_TAK001.EK_TPF,
      us_mo_LBAKRDTA_TAK001.EK_NM,
      us_mo_LBAKRDTA_TAK001.EK_TAT,
      us_mo_LBAKRDTA_TAK001.EK_WRF,
      us_mo_LBAKRDTA_TAK001.EK_DTF,
      us_mo_LBAKRDTA_TAK001.EK_WTF,
      us_mo_LBAKRDTA_TAK001.EK_SOQ,
      us_mo_LBAKRDTA_TAK001.EK_RAC,
      us_mo_LBAKRDTA_TAK001.EK_ETH,
      us_mo_LBAKRDTA_TAK001.EK_SEX,
      us_mo_LBAKRDTA_TAK001.EK_HTF,
      us_mo_LBAKRDTA_TAK001.EK_HTI,
      us_mo_LBAKRDTA_TAK001.EK_WGT,
      us_mo_LBAKRDTA_TAK001.EK_BIL,
      us_mo_LBAKRDTA_TAK001.EK_HAI,
      us_mo_LBAKRDTA_TAK001.EK_EYE,
      us_mo_LBAKRDTA_TAK001.EK_SKI,
      us_mo_LBAKRDTA_TAK001.EK_MAS,
      us_mo_LBAKRDTA_TAK001.EK_DEP,
      us_mo_LBAKRDTA_TAK001.EK_SIB,
      us_mo_LBAKRDTA_TAK001.EK_REL,
      us_mo_LBAKRDTA_TAK001.EK_COF,
      us_mo_LBAKRDTA_TAK001.EK_SCO,
      us_mo_LBAKRDTA_TAK001.EK_XDM,
      us_mo_LBAKRDTA_TAK001.EK_XDO,
      us_mo_LBAKRDTA_TAK001.EK_XEM,
      us_mo_LBAKRDTA_TAK001.EK_XEO,
      us_mo_LBAKRDTA_TAK001.EK_XPM,
      us_mo_LBAKRDTA_TAK001.EK_XPO,
      us_mo_LBAKRDTA_TAK001.EK_XCM,
      us_mo_LBAKRDTA_TAK001.EK_XCO,
      us_mo_LBAKRDTA_TAK001.EK_XBM,
      us_mo_LBAKRDTA_TAK001.EK_XBO,
      us_mo_LBAKRDTA_TAK001.EK_PU,
      us_mo_LBAKRDTA_TAK001.EK_PUL,
      us_mo_LBAKRDTA_TAK001.EK_PRF,
      us_mo_LBAKRDTA_TAK001.EK_DCR,
      us_mo_LBAKRDTA_TAK001.EK_TCR,
      us_mo_LBAKRDTA_TAK001.EK_DLU,
      us_mo_LBAKRDTA_TAK001.EK_TLU,
      us_mo_LBAKRDTA_TAK001.EK_REA,
      us_mo_LBAKRDTA_TAK001.EK_UID,
      us_mo_LBAKRDTA_TAK001.file_id,
      us_mo_LBAKRDTA_TAK001.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK001.EK_DOC]
    note_display: hover
    note_text: "Offender Identification. Column prefix: EK."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 0
    col: 0
    width: 24
    height: 6

  - name: ORAS_WEEKLY_SUMMARY_UPDATE
    title: ORAS_WEEKLY_SUMMARY_UPDATE
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.primary_key,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.OFFENDER_NAME,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.AGENCY_NAME,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.DATE_OF_BIRTH,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.GENDER,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.ETHNICITY,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.DOC_ID,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.ASSESSMENT_TYPE,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.RISK_LEVEL,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.OVERRIDE_RISK_LEVEL,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.OVERRIDE_RISK_REASON,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.ASSESSMENT_OUTCOME,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.ASSESSMENT_STATUS,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.SCORE,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.DATE_CREATED,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.USER_CREATED,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.RACE,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.CREATED_DATE__raw,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.file_id,
      us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.is_deleted]
    sorts: [us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.CREATED_DATE__raw]
    note_display: hover
    note_text: "ORAS assessment scores (updated weekly)"
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 6
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK015
    title: LBAKRDTA_TAK015
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK015.primary_key,
      us_mo_LBAKRDTA_TAK015.BL_DOC,
      us_mo_LBAKRDTA_TAK015.BL_CYC,
      us_mo_LBAKRDTA_TAK015.BL_CNO,
      us_mo_LBAKRDTA_TAK015.BL_CAT,
      us_mo_LBAKRDTA_TAK015.BL_CAV,
      us_mo_LBAKRDTA_TAK015.BL_ICA,
      us_mo_LBAKRDTA_TAK015.BL_IC__raw,
      us_mo_LBAKRDTA_TAK015.BL_ICO,
      us_mo_LBAKRDTA_TAK015.BL_OD,
      us_mo_LBAKRDTA_TAK015.BL_PON,
      us_mo_LBAKRDTA_TAK015.BL_NH,
      us_mo_LBAKRDTA_TAK015.BL_CSQ,
      us_mo_LBAKRDTA_TAK015.BL_UID,
      us_mo_LBAKRDTA_TAK015.BL_DCR,
      us_mo_LBAKRDTA_TAK015.BL_TCR,
      us_mo_LBAKRDTA_TAK015.BL_UIU,
      us_mo_LBAKRDTA_TAK015.BL_DLU,
      us_mo_LBAKRDTA_TAK015.BL_TLU,
      us_mo_LBAKRDTA_TAK015.file_id,
      us_mo_LBAKRDTA_TAK015.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK015.BL_IC__raw]
    note_display: hover
    note_text: "Custody Level Details. Column prefix: BL. # TODO(#13474) Fill in definition with more details."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 12
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK017
    title: LBAKRDTA_TAK017
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK017.primary_key,
      us_mo_LBAKRDTA_TAK017.BN_DOC,
      us_mo_LBAKRDTA_TAK017.BN_CYC,
      us_mo_LBAKRDTA_TAK017.BN_OR0,
      us_mo_LBAKRDTA_TAK017.BN_HE__raw,
      us_mo_LBAKRDTA_TAK017.BN_HS__raw,
      us_mo_LBAKRDTA_TAK017.BN_TCR,
      us_mo_LBAKRDTA_TAK017.BN_DCR,
      us_mo_LBAKRDTA_TAK017.BN_PIN,
      us_mo_LBAKRDTA_TAK017.BN_PLN,
      us_mo_LBAKRDTA_TAK017.BN_HPT,
      us_mo_LBAKRDTA_TAK017.BN_LOC,
      us_mo_LBAKRDTA_TAK017.BN_COM,
      us_mo_LBAKRDTA_TAK017.BN_LRM,
      us_mo_LBAKRDTA_TAK017.BN_LBD,
      us_mo_LBAKRDTA_TAK017.BN_LRU,
      us_mo_LBAKRDTA_TAK017.BN_HDS,
      us_mo_LBAKRDTA_TAK017.BN_DLU,
      us_mo_LBAKRDTA_TAK017.BN_TLU,
      us_mo_LBAKRDTA_TAK017.file_id,
      us_mo_LBAKRDTA_TAK017.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK017.BN_HE__raw]
    note_display: hover
    note_text: "Housing Details. Column prefix: BN. # TODO(#13474) Fill in definition with more details."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 18
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK020
    title: LBAKRDTA_TAK020
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK020.primary_key,
      us_mo_LBAKRDTA_TAK020.BQ_DOC,
      us_mo_LBAKRDTA_TAK020.BQ_CYC,
      us_mo_LBAKRDTA_TAK020.BQ_BSN,
      us_mo_LBAKRDTA_TAK020.BQ_BAV,
      us_mo_LBAKRDTA_TAK020.BQ_PBA,
      us_mo_LBAKRDTA_TAK020.BQ_PH,
      us_mo_LBAKRDTA_TAK020.BQ_HRN,
      us_mo_LBAKRDTA_TAK020.BQ_TPN,
      us_mo_LBAKRDTA_TAK020.BQ_GUD,
      us_mo_LBAKRDTA_TAK020.BQ_PRV,
      us_mo_LBAKRDTA_TAK020.BQ_PR,
      us_mo_LBAKRDTA_TAK020.BQ_RRS,
      us_mo_LBAKRDTA_TAK020.BQ_SCN,
      us_mo_LBAKRDTA_TAK020.BQ_REF,
      us_mo_LBAKRDTA_TAK020.BQ_PDS,
      us_mo_LBAKRDTA_TAK020.BQ_ESN,
      us_mo_LBAKRDTA_TAK020.BQ_SDS,
      us_mo_LBAKRDTA_TAK020.BQ_SEO,
      us_mo_LBAKRDTA_TAK020.BQ_SOC,
      us_mo_LBAKRDTA_TAK020.BQ_RFB,
      us_mo_LBAKRDTA_TAK020.BQ_VCN,
      us_mo_LBAKRDTA_TAK020.BQ_VCP,
      us_mo_LBAKRDTA_TAK020.BQ_OFN,
      us_mo_LBAKRDTA_TAK020.BQ_OFP,
      us_mo_LBAKRDTA_TAK020.BQ_PBN,
      us_mo_LBAKRDTA_TAK020.BQ_NA,
      us_mo_LBAKRDTA_TAK020.BQ_PA,
      us_mo_LBAKRDTA_TAK020.BQ_HRF,
      us_mo_LBAKRDTA_TAK020.BQ_RTC,
      us_mo_LBAKRDTA_TAK020.BQ_RV,
      us_mo_LBAKRDTA_TAK020.BQ_OAP,
      us_mo_LBAKRDTA_TAK020.BQ_AO,
      us_mo_LBAKRDTA_TAK020.BQ_OR,
      us_mo_LBAKRDTA_TAK020.BQ_OM,
      us_mo_LBAKRDTA_TAK020.BQ_ON,
      us_mo_LBAKRDTA_TAK020.BQ_ABF,
      us_mo_LBAKRDTA_TAK020.BQ_SOF,
      us_mo_LBAKRDTA_TAK020.BQ_SS,
      us_mo_LBAKRDTA_TAK020.BQ_DCR,
      us_mo_LBAKRDTA_TAK020.BQ_TCR,
      us_mo_LBAKRDTA_TAK020.BQ_DLU,
      us_mo_LBAKRDTA_TAK020.BQ_TLU,
      us_mo_LBAKRDTA_TAK020.file_id,
      us_mo_LBAKRDTA_TAK020.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK020.BQ_DOC, us_mo_LBAKRDTA_TAK020.BQ_CYC, us_mo_LBAKRDTA_TAK020.BQ_BSN]
    note_display: hover
    note_text: "Parole Board Actions. Column prefix: BQ # TODO(#13474) Fill in definition with more details."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 24
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK022
    title: LBAKRDTA_TAK022
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK022.primary_key,
      us_mo_LBAKRDTA_TAK022.BS_DOC,
      us_mo_LBAKRDTA_TAK022.BS_CYC,
      us_mo_LBAKRDTA_TAK022.BS_SEO,
      us_mo_LBAKRDTA_TAK022.BS_LEO,
      us_mo_LBAKRDTA_TAK022.BS_SCF,
      us_mo_LBAKRDTA_TAK022.BS_CRT,
      us_mo_LBAKRDTA_TAK022.BS_NRN,
      us_mo_LBAKRDTA_TAK022.BS_ASO,
      us_mo_LBAKRDTA_TAK022.BS_NCI,
      us_mo_LBAKRDTA_TAK022.BS_OCN,
      us_mo_LBAKRDTA_TAK022.BS_CLT,
      us_mo_LBAKRDTA_TAK022.BS_CNT,
      us_mo_LBAKRDTA_TAK022.BS_CLA,
      us_mo_LBAKRDTA_TAK022.BS_POF,
      us_mo_LBAKRDTA_TAK022.BS_ACL,
      us_mo_LBAKRDTA_TAK022.BS_CCI,
      us_mo_LBAKRDTA_TAK022.BS_CRQ,
      us_mo_LBAKRDTA_TAK022.BS_CNS,
      us_mo_LBAKRDTA_TAK022.BS_CRC,
      us_mo_LBAKRDTA_TAK022.BS_CRD,
      us_mo_LBAKRDTA_TAK022.BS_PD,
      us_mo_LBAKRDTA_TAK022.BS_DO,
      us_mo_LBAKRDTA_TAK022.BS_PLE,
      us_mo_LBAKRDTA_TAK022.BS_COD,
      us_mo_LBAKRDTA_TAK022.BS_AR,
      us_mo_LBAKRDTA_TAK022.BS_UID,
      us_mo_LBAKRDTA_TAK022.BS_DCR,
      us_mo_LBAKRDTA_TAK022.BS_TCR,
      us_mo_LBAKRDTA_TAK022.BS_UIU,
      us_mo_LBAKRDTA_TAK022.BS_DLU,
      us_mo_LBAKRDTA_TAK022.BS_TLU,
      us_mo_LBAKRDTA_TAK022.file_id,
      us_mo_LBAKRDTA_TAK022.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK022.BS_DOC, us_mo_LBAKRDTA_TAK022.BS_CYC, us_mo_LBAKRDTA_TAK022.BS_SEO]
    note_display: hover
    note_text: "Sentence. Column prefix: BS."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 30
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK023
    title: LBAKRDTA_TAK023
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK023.primary_key,
      us_mo_LBAKRDTA_TAK023.BT_DOC,
      us_mo_LBAKRDTA_TAK023.BT_CYC,
      us_mo_LBAKRDTA_TAK023.BT_SEO,
      us_mo_LBAKRDTA_TAK023.BT_SD,
      us_mo_LBAKRDTA_TAK023.BT_SLY,
      us_mo_LBAKRDTA_TAK023.BT_SLM,
      us_mo_LBAKRDTA_TAK023.BT_SLD,
      us_mo_LBAKRDTA_TAK023.BT_CRR,
      us_mo_LBAKRDTA_TAK023.BT_PC,
      us_mo_LBAKRDTA_TAK023.BT_ABS,
      us_mo_LBAKRDTA_TAK023.BT_ABU,
      us_mo_LBAKRDTA_TAK023.BT_ABT,
      us_mo_LBAKRDTA_TAK023.BT_PIE,
      us_mo_LBAKRDTA_TAK023.BT_SRC,
      us_mo_LBAKRDTA_TAK023.BT_SRF,
      us_mo_LBAKRDTA_TAK023.BT_PCR,
      us_mo_LBAKRDTA_TAK023.BT_EM,
      us_mo_LBAKRDTA_TAK023.BT_OTD,
      us_mo_LBAKRDTA_TAK023.BT_OH,
      us_mo_LBAKRDTA_TAK023.BT_SCT,
      us_mo_LBAKRDTA_TAK023.BT_RE,
      us_mo_LBAKRDTA_TAK023.BT_SDI,
      us_mo_LBAKRDTA_TAK023.BT_DCR,
      us_mo_LBAKRDTA_TAK023.BT_TCR,
      us_mo_LBAKRDTA_TAK023.BT_DLU,
      us_mo_LBAKRDTA_TAK023.BT_TLU,
      us_mo_LBAKRDTA_TAK023.file_id,
      us_mo_LBAKRDTA_TAK023.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK023.BT_DOC, us_mo_LBAKRDTA_TAK023.BT_CYC, us_mo_LBAKRDTA_TAK023.BT_SEO]
    note_display: hover
    note_text: "Sentence Inst. Column prefix: BT."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 36
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK024
    title: LBAKRDTA_TAK024
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK024.primary_key,
      us_mo_LBAKRDTA_TAK024.BU_DOC,
      us_mo_LBAKRDTA_TAK024.BU_CYC,
      us_mo_LBAKRDTA_TAK024.BU_SEO,
      us_mo_LBAKRDTA_TAK024.BU_FSO,
      us_mo_LBAKRDTA_TAK024.BU_SF,
      us_mo_LBAKRDTA_TAK024.BU_SBY,
      us_mo_LBAKRDTA_TAK024.BU_SBM,
      us_mo_LBAKRDTA_TAK024.BU_SBD,
      us_mo_LBAKRDTA_TAK024.BU_PBT,
      us_mo_LBAKRDTA_TAK024.BU_SLY,
      us_mo_LBAKRDTA_TAK024.BU_SLM,
      us_mo_LBAKRDTA_TAK024.BU_SLD,
      us_mo_LBAKRDTA_TAK024.BU_SAI,
      us_mo_LBAKRDTA_TAK024.BU_EMP,
      us_mo_LBAKRDTA_TAK024.BU_FRC,
      us_mo_LBAKRDTA_TAK024.BU_WEA,
      us_mo_LBAKRDTA_TAK024.BU_DEF,
      us_mo_LBAKRDTA_TAK024.BU_DCR,
      us_mo_LBAKRDTA_TAK024.BU_TCR,
      us_mo_LBAKRDTA_TAK024.BU_DLU,
      us_mo_LBAKRDTA_TAK024.BU_TLU,
      us_mo_LBAKRDTA_TAK024.file_id,
      us_mo_LBAKRDTA_TAK024.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK024.BU_DOC, us_mo_LBAKRDTA_TAK024.BU_CYC, us_mo_LBAKRDTA_TAK024.BU_SEO, us_mo_LBAKRDTA_TAK024.BU_FSO]
    note_display: hover
    note_text: "Sentence Prob. Column prefix: BU."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 42
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK025
    title: LBAKRDTA_TAK025
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK025.primary_key,
      us_mo_LBAKRDTA_TAK025.BV_DOC,
      us_mo_LBAKRDTA_TAK025.BV_CYC,
      us_mo_LBAKRDTA_TAK025.BV_SSO,
      us_mo_LBAKRDTA_TAK025.BV_SEO,
      us_mo_LBAKRDTA_TAK025.BV_FSO,
      us_mo_LBAKRDTA_TAK025.BV_DCR,
      us_mo_LBAKRDTA_TAK025.BV_TCR,
      us_mo_LBAKRDTA_TAK025.BV_DLU,
      us_mo_LBAKRDTA_TAK025.BV_TLU,
      us_mo_LBAKRDTA_TAK025.file_id,
      us_mo_LBAKRDTA_TAK025.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK025.BV_DOC, us_mo_LBAKRDTA_TAK025.BV_CYC, us_mo_LBAKRDTA_TAK025.BV_SSO, us_mo_LBAKRDTA_TAK025.BV_SEO]
    note_display: hover
    note_text: "Sentence / Status Xref. Column prefix: BV."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 48
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK026
    title: LBAKRDTA_TAK026
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK026.primary_key,
      us_mo_LBAKRDTA_TAK026.BW_DOC,
      us_mo_LBAKRDTA_TAK026.BW_CYC,
      us_mo_LBAKRDTA_TAK026.BW_SSO,
      us_mo_LBAKRDTA_TAK026.BW_SCD,
      us_mo_LBAKRDTA_TAK026.BW_SY__raw,
      us_mo_LBAKRDTA_TAK026.BW_SM,
      us_mo_LBAKRDTA_TAK026.BW_CSC,
      us_mo_LBAKRDTA_TAK026.BW_DCR,
      us_mo_LBAKRDTA_TAK026.BW_TCR,
      us_mo_LBAKRDTA_TAK026.BW_DLU,
      us_mo_LBAKRDTA_TAK026.BW_TLU,
      us_mo_LBAKRDTA_TAK026.file_id,
      us_mo_LBAKRDTA_TAK026.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK026.BW_SY__raw]
    note_display: hover
    note_text: "Status. Column prefix: BW."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 54
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK158
    title: LBAKRDTA_TAK158
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK158.primary_key,
      us_mo_LBAKRDTA_TAK158.F1_DOC,
      us_mo_LBAKRDTA_TAK158.F1_CYC,
      us_mo_LBAKRDTA_TAK158.F1_SQN,
      us_mo_LBAKRDTA_TAK158.F1_SST,
      us_mo_LBAKRDTA_TAK158.F1_CD__raw,
      us_mo_LBAKRDTA_TAK158.F1_ORC,
      us_mo_LBAKRDTA_TAK158.F1_CTO,
      us_mo_LBAKRDTA_TAK158.F1_OPT,
      us_mo_LBAKRDTA_TAK158.F1_CTC,
      us_mo_LBAKRDTA_TAK158.F1_SY__raw,
      us_mo_LBAKRDTA_TAK158.F1_CTP,
      us_mo_LBAKRDTA_TAK158.F1_ARC,
      us_mo_LBAKRDTA_TAK158.F1_PFI,
      us_mo_LBAKRDTA_TAK158.F1_OR0,
      us_mo_LBAKRDTA_TAK158.F1_WW,
      us_mo_LBAKRDTA_TAK158.F1_MSO,
      us_mo_LBAKRDTA_TAK158.F1_SEO,
      us_mo_LBAKRDTA_TAK158.F1_DCR,
      us_mo_LBAKRDTA_TAK158.F1_TCR,
      us_mo_LBAKRDTA_TAK158.file_id,
      us_mo_LBAKRDTA_TAK158.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK158.F1_CD__raw]
    note_display: hover
    note_text: "Body Status. Column prefix: F1."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 60
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK065
    title: LBAKRDTA_TAK065
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK065.primary_key,
      us_mo_LBAKRDTA_TAK065.CS_DOC,
      us_mo_LBAKRDTA_TAK065.CS_CYC,
      us_mo_LBAKRDTA_TAK065.CS_OLC,
      us_mo_LBAKRDTA_TAK065.CS_OLA,
      us_mo_LBAKRDTA_TAK065.CS_PLC,
      us_mo_LBAKRDTA_TAK065.CS_PLA,
      us_mo_LBAKRDTA_TAK065.CS_FLC,
      us_mo_LBAKRDTA_TAK065.CS_FLA,
      us_mo_LBAKRDTA_TAK065.CS_LTR,
      us_mo_LBAKRDTA_TAK065.CS_REA,
      us_mo_LBAKRDTA_TAK065.CS_AV,
      us_mo_LBAKRDTA_TAK065.CS_NM__raw,
      us_mo_LBAKRDTA_TAK065.CS_DD__raw,
      us_mo_LBAKRDTA_TAK065.CS_TAT,
      us_mo_LBAKRDTA_TAK065.CS_TDT,
      us_mo_LBAKRDTA_TAK065.CS_DCR,
      us_mo_LBAKRDTA_TAK065.CS_TCR,
      us_mo_LBAKRDTA_TAK065.CS_DLU,
      us_mo_LBAKRDTA_TAK065.CS_TLU,
      us_mo_LBAKRDTA_TAK065.CS_UID,
      us_mo_LBAKRDTA_TAK065.file_id,
      us_mo_LBAKRDTA_TAK065.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK065.CS_NM__raw]
    note_display: hover
    note_text: "Institution History. Column prefix: CS."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 66
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK028
    title: LBAKRDTA_TAK028
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK028.primary_key,
      us_mo_LBAKRDTA_TAK028.BY_DOC,
      us_mo_LBAKRDTA_TAK028.BY_CYC,
      us_mo_LBAKRDTA_TAK028.BY_VSN,
      us_mo_LBAKRDTA_TAK028.BY_VE,
      us_mo_LBAKRDTA_TAK028.BY_VWI,
      us_mo_LBAKRDTA_TAK028.BY_VRT,
      us_mo_LBAKRDTA_TAK028.BY_VSI,
      us_mo_LBAKRDTA_TAK028.BY_VPH,
      us_mo_LBAKRDTA_TAK028.BY_VBG,
      us_mo_LBAKRDTA_TAK028.BY_VA,
      us_mo_LBAKRDTA_TAK028.BY_VIC,
      us_mo_LBAKRDTA_TAK028.BY_DAX,
      us_mo_LBAKRDTA_TAK028.BY_VC,
      us_mo_LBAKRDTA_TAK028.BY_VD,
      us_mo_LBAKRDTA_TAK028.BY_VIH,
      us_mo_LBAKRDTA_TAK028.BY_VIM,
      us_mo_LBAKRDTA_TAK028.BY_VIL,
      us_mo_LBAKRDTA_TAK028.BY_VOR,
      us_mo_LBAKRDTA_TAK028.BY_PIN,
      us_mo_LBAKRDTA_TAK028.BY_PLN,
      us_mo_LBAKRDTA_TAK028.BY_PON,
      us_mo_LBAKRDTA_TAK028.BY_RCA,
      us_mo_LBAKRDTA_TAK028.BY_VTY,
      us_mo_LBAKRDTA_TAK028.BY_DV__raw,
      us_mo_LBAKRDTA_TAK028.BY_DCR,
      us_mo_LBAKRDTA_TAK028.BY_TCR,
      us_mo_LBAKRDTA_TAK028.BY_UID,
      us_mo_LBAKRDTA_TAK028.BY_DLU,
      us_mo_LBAKRDTA_TAK028.BY_TLU,
      us_mo_LBAKRDTA_TAK028.BY_UIU,
      us_mo_LBAKRDTA_TAK028.file_id,
      us_mo_LBAKRDTA_TAK028.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK028.BY_DV__raw]
    note_display: hover
    note_text: "Violation Reports. Column prefix: BY."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 72
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK034
    title: LBAKRDTA_TAK034
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK034.primary_key,
      us_mo_LBAKRDTA_TAK034.CE_DOC,
      us_mo_LBAKRDTA_TAK034.CE_CYC,
      us_mo_LBAKRDTA_TAK034.CE_HF__raw,
      us_mo_LBAKRDTA_TAK034.CE_OR0,
      us_mo_LBAKRDTA_TAK034.CE_EH__raw,
      us_mo_LBAKRDTA_TAK034.CE_PIN,
      us_mo_LBAKRDTA_TAK034.CE_PLN,
      us_mo_LBAKRDTA_TAK034.CE_PON,
      us_mo_LBAKRDTA_TAK034.CE_DCR,
      us_mo_LBAKRDTA_TAK034.CE_TCR,
      us_mo_LBAKRDTA_TAK034.CE_DLU,
      us_mo_LBAKRDTA_TAK034.CE_TLU,
      us_mo_LBAKRDTA_TAK034.file_id,
      us_mo_LBAKRDTA_TAK034.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK034.CE_HF__raw]
    note_display: hover
    note_text: "Field Assignments. Column prefix: CE."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 78
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK039
    title: LBAKRDTA_TAK039
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK039.primary_key,
      us_mo_LBAKRDTA_TAK039.DN_DOC,
      us_mo_LBAKRDTA_TAK039.DN_CYC,
      us_mo_LBAKRDTA_TAK039.DN_NSN,
      us_mo_LBAKRDTA_TAK039.DN_PIN,
      us_mo_LBAKRDTA_TAK039.DN_PLN,
      us_mo_LBAKRDTA_TAK039.DN_PON,
      us_mo_LBAKRDTA_TAK039.DN_NED,
      us_mo_LBAKRDTA_TAK039.DN_RC__raw,
      us_mo_LBAKRDTA_TAK039.DN_NSV,
      us_mo_LBAKRDTA_TAK039.DN_DA,
      us_mo_LBAKRDTA_TAK039.DN_DP,
      us_mo_LBAKRDTA_TAK039.DN_AB,
      us_mo_LBAKRDTA_TAK039.DN_UAS,
      us_mo_LBAKRDTA_TAK039.DN_POS,
      us_mo_LBAKRDTA_TAK039.DN_SFP,
      us_mo_LBAKRDTA_TAK039.DN_SOR,
      us_mo_LBAKRDTA_TAK039.DN_PST,
      us_mo_LBAKRDTA_TAK039.DN_A01,
      us_mo_LBAKRDTA_TAK039.DN_A02,
      us_mo_LBAKRDTA_TAK039.DN_A03,
      us_mo_LBAKRDTA_TAK039.DN_A04,
      us_mo_LBAKRDTA_TAK039.DN_A05,
      us_mo_LBAKRDTA_TAK039.DN_A06,
      us_mo_LBAKRDTA_TAK039.DN_A07,
      us_mo_LBAKRDTA_TAK039.DN_A08,
      us_mo_LBAKRDTA_TAK039.DN_A09,
      us_mo_LBAKRDTA_TAK039.DN_A10,
      us_mo_LBAKRDTA_TAK039.DN_A11,
      us_mo_LBAKRDTA_TAK039.DN_A12,
      us_mo_LBAKRDTA_TAK039.DN_S01,
      us_mo_LBAKRDTA_TAK039.DN_S02,
      us_mo_LBAKRDTA_TAK039.DN_S03,
      us_mo_LBAKRDTA_TAK039.DN_S04,
      us_mo_LBAKRDTA_TAK039.DN_S05,
      us_mo_LBAKRDTA_TAK039.DN_S06,
      us_mo_LBAKRDTA_TAK039.DN_S07,
      us_mo_LBAKRDTA_TAK039.DN_S08,
      us_mo_LBAKRDTA_TAK039.DN_S09,
      us_mo_LBAKRDTA_TAK039.DN_S10,
      us_mo_LBAKRDTA_TAK039.DN_S11,
      us_mo_LBAKRDTA_TAK039.DN_S12,
      us_mo_LBAKRDTA_TAK039.DN_DCR,
      us_mo_LBAKRDTA_TAK039.DN_TCR,
      us_mo_LBAKRDTA_TAK039.DN_DLU,
      us_mo_LBAKRDTA_TAK039.DN_TLU,
      us_mo_LBAKRDTA_TAK039.file_id,
      us_mo_LBAKRDTA_TAK039.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK039.DN_RC__raw]
    note_display: hover
    note_text: "Supervision Type Assessments. Column prefix: DN."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 84
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK042
    title: LBAKRDTA_TAK042
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK042.primary_key,
      us_mo_LBAKRDTA_TAK042.CF_DOC,
      us_mo_LBAKRDTA_TAK042.CF_CYC,
      us_mo_LBAKRDTA_TAK042.CF_VSN,
      us_mo_LBAKRDTA_TAK042.CF_TSS,
      us_mo_LBAKRDTA_TAK042.CF_VCV,
      us_mo_LBAKRDTA_TAK042.CF_NBR,
      us_mo_LBAKRDTA_TAK042.CF_DCR,
      us_mo_LBAKRDTA_TAK042.CF_TCR,
      us_mo_LBAKRDTA_TAK042.CF_DLU,
      us_mo_LBAKRDTA_TAK042.CF_TLU,
      us_mo_LBAKRDTA_TAK042.file_id,
      us_mo_LBAKRDTA_TAK042.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK042.CF_DOC, us_mo_LBAKRDTA_TAK042.CF_CYC, us_mo_LBAKRDTA_TAK042.CF_VSN, us_mo_LBAKRDTA_TAK042.CF_VCV]
    note_display: hover
    note_text: "Conditions. Column prefix: CF."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 90
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK044
    title: LBAKRDTA_TAK044
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK044.primary_key,
      us_mo_LBAKRDTA_TAK044.CG_DOC,
      us_mo_LBAKRDTA_TAK044.CG_CYC,
      us_mo_LBAKRDTA_TAK044.CG_ESN,
      us_mo_LBAKRDTA_TAK044.CG_RC,
      us_mo_LBAKRDTA_TAK044.CG_PON,
      us_mo_LBAKRDTA_TAK044.CG_PIN,
      us_mo_LBAKRDTA_TAK044.CG_PLN,
      us_mo_LBAKRDTA_TAK044.CG_FML,
      us_mo_LBAKRDTA_TAK044.CG_MD,
      us_mo_LBAKRDTA_TAK044.CG_GL,
      us_mo_LBAKRDTA_TAK044.CG_GD,
      us_mo_LBAKRDTA_TAK044.CG_GT,
      us_mo_LBAKRDTA_TAK044.CG_RR,
      us_mo_LBAKRDTA_TAK044.CG_RF,
      us_mo_LBAKRDTA_TAK044.CG_RT,
      us_mo_LBAKRDTA_TAK044.CG_MM,
      us_mo_LBAKRDTA_TAK044.CG_MMP,
      us_mo_LBAKRDTA_TAK044.CG_DCR,
      us_mo_LBAKRDTA_TAK044.CG_TCR,
      us_mo_LBAKRDTA_TAK044.CG_DLU,
      us_mo_LBAKRDTA_TAK044.CG_TLU,
      us_mo_LBAKRDTA_TAK044.file_id,
      us_mo_LBAKRDTA_TAK044.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK044.CG_DOC, us_mo_LBAKRDTA_TAK044.CG_CYC, us_mo_LBAKRDTA_TAK044.CG_ESN]
    note_display: hover
    note_text: "IMEG Screen in MOCIS - Shows minimum eligibility data. Column prefix: CG."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 96
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK076
    title: LBAKRDTA_TAK076
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK076.primary_key,
      us_mo_LBAKRDTA_TAK076.CZ_DOC,
      us_mo_LBAKRDTA_TAK076.CZ_CYC,
      us_mo_LBAKRDTA_TAK076.CZ_VSN,
      us_mo_LBAKRDTA_TAK076.CZ_SEO,
      us_mo_LBAKRDTA_TAK076.CZ_FSO,
      us_mo_LBAKRDTA_TAK076.CZ_DCR,
      us_mo_LBAKRDTA_TAK076.CZ_TCR,
      us_mo_LBAKRDTA_TAK076.CZ_DLU,
      us_mo_LBAKRDTA_TAK076.CZ_TLU,
      us_mo_LBAKRDTA_TAK076.file_id,
      us_mo_LBAKRDTA_TAK076.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK076.CZ_DOC, us_mo_LBAKRDTA_TAK076.CZ_CYC, us_mo_LBAKRDTA_TAK076.CZ_VSN, us_mo_LBAKRDTA_TAK076.CZ_SEO]
    note_display: hover
    note_text: "Sentence / Violation Xref. Column prefix: CZ."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 102
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK142
    title: LBAKRDTA_TAK142
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK142.primary_key,
      us_mo_LBAKRDTA_TAK142.E6_DOC,
      us_mo_LBAKRDTA_TAK142.E6_CYC,
      us_mo_LBAKRDTA_TAK142.E6_DON,
      us_mo_LBAKRDTA_TAK142.E6_DOS,
      us_mo_LBAKRDTA_TAK142.E6_FFS,
      us_mo_LBAKRDTA_TAK142.E6_DSN,
      us_mo_LBAKRDTA_TAK142.E6_DIN,
      us_mo_LBAKRDTA_TAK142.E6_UID,
      us_mo_LBAKRDTA_TAK142.E6_DCR,
      us_mo_LBAKRDTA_TAK142.E6_TCR,
      us_mo_LBAKRDTA_TAK142.E6_DLU,
      us_mo_LBAKRDTA_TAK142.E6_TLU,
      us_mo_LBAKRDTA_TAK142.E6_DCV,
      us_mo_LBAKRDTA_TAK142.file_id,
      us_mo_LBAKRDTA_TAK142.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK142.E6_DOC, us_mo_LBAKRDTA_TAK142.E6_CYC, us_mo_LBAKRDTA_TAK142.E6_DSN]
    note_display: hover
    note_text: "Finally Formed Documents. Column prefix: E6."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 108
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK233
    title: LBAKRDTA_TAK233
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK233.primary_key,
      us_mo_LBAKRDTA_TAK233.IZ_DOC,
      us_mo_LBAKRDTA_TAK233.IZ_CYC,
      us_mo_LBAKRDTA_TAK233.IZCSEQ,
      us_mo_LBAKRDTA_TAK233.IZWDTE,
      us_mo_LBAKRDTA_TAK233.IZVRUL,
      us_mo_LBAKRDTA_TAK233.IZ_II,
      us_mo_LBAKRDTA_TAK233.IZVTIM,
      us_mo_LBAKRDTA_TAK233.IZ_PON,
      us_mo_LBAKRDTA_TAK233.IZ_MO1,
      us_mo_LBAKRDTA_TAK233.IZTPRE,
      us_mo_LBAKRDTA_TAK233.IZCTRK,
      us_mo_LBAKRDTA_TAK233.IZCSTS,
      us_mo_LBAKRDTA_TAK233.IZ_PLN,
      us_mo_LBAKRDTA_TAK233.IZ_PIN,
      us_mo_LBAKRDTA_TAK233.IZ_LOC,
      us_mo_LBAKRDTA_TAK233.IZ_COM,
      us_mo_LBAKRDTA_TAK233.IZ_LRM,
      us_mo_LBAKRDTA_TAK233.IZCSQ_,
      us_mo_LBAKRDTA_TAK233.IZHPLN,
      us_mo_LBAKRDTA_TAK233.IZHPIN,
      us_mo_LBAKRDTA_TAK233.IZHLOC,
      us_mo_LBAKRDTA_TAK233.IZHCOM,
      us_mo_LBAKRDTA_TAK233.IZHLRM,
      us_mo_LBAKRDTA_TAK233.IZHLBD,
      us_mo_LBAKRDTA_TAK233.IZ_WSN,
      us_mo_LBAKRDTA_TAK233.IZ_DCR,
      us_mo_LBAKRDTA_TAK233.IZ_TCR,
      us_mo_LBAKRDTA_TAK233.IZ_DLU,
      us_mo_LBAKRDTA_TAK233.IZ_TLU,
      us_mo_LBAKRDTA_TAK233.IZ_UID,
      us_mo_LBAKRDTA_TAK233.IZ_UIU,
      us_mo_LBAKRDTA_TAK233.file_id,
      us_mo_LBAKRDTA_TAK233.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK233.IZ_DOC, us_mo_LBAKRDTA_TAK233.IZ_CYC, us_mo_LBAKRDTA_TAK233.IZCSEQ]
    note_display: hover
    note_text: "Violations data information (CDV). Column prefix: IZ."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 114
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK236
    title: LBAKRDTA_TAK236
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK236.primary_key,
      us_mo_LBAKRDTA_TAK236.IU_DOC,
      us_mo_LBAKRDTA_TAK236.IU_CYC,
      us_mo_LBAKRDTA_TAK236.IUCSEQ,
      us_mo_LBAKRDTA_TAK236.IUSASQ,
      us_mo_LBAKRDTA_TAK236.IUSSAN,
      us_mo_LBAKRDTA_TAK236.IU_SU,
      us_mo_LBAKRDTA_TAK236.IU_SE,
      us_mo_LBAKRDTA_TAK236.IUSMTH,
      us_mo_LBAKRDTA_TAK236.IU_SAD,
      us_mo_LBAKRDTA_TAK236.IU_SHR,
      us_mo_LBAKRDTA_TAK236.IUSAMO,
      us_mo_LBAKRDTA_TAK236.IU_SPD,
      us_mo_LBAKRDTA_TAK236.IU_DCR,
      us_mo_LBAKRDTA_TAK236.IU_TCR,
      us_mo_LBAKRDTA_TAK236.IU_DLU,
      us_mo_LBAKRDTA_TAK236.IU_TLU,
      us_mo_LBAKRDTA_TAK236.IU_UID,
      us_mo_LBAKRDTA_TAK236.IU_UIU,
      us_mo_LBAKRDTA_TAK236.file_id,
      us_mo_LBAKRDTA_TAK236.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK236.IU_DOC, us_mo_LBAKRDTA_TAK236.IU_CYC, us_mo_LBAKRDTA_TAK236.IUCSEQ, us_mo_LBAKRDTA_TAK236.IUSASQ]
    note_display: hover
    note_text: "Violations data information (CDV) - Sanctions. Column prefix: IU."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 120
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK237
    title: LBAKRDTA_TAK237
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK237.primary_key,
      us_mo_LBAKRDTA_TAK237.IV_DOC,
      us_mo_LBAKRDTA_TAK237.IV_CYC,
      us_mo_LBAKRDTA_TAK237.IVCSEQ,
      us_mo_LBAKRDTA_TAK237.IVESEQ,
      us_mo_LBAKRDTA_TAK237.IVETYP,
      us_mo_LBAKRDTA_TAK237.IVEDTE,
      us_mo_LBAKRDTA_TAK237.IVETIM,
      us_mo_LBAKRDTA_TAK237.IV_PON,
      us_mo_LBAKRDTA_TAK237.IV_FPC,
      us_mo_LBAKRDTA_TAK237.IV_FFD,
      us_mo_LBAKRDTA_TAK237.IVRAST,
      us_mo_LBAKRDTA_TAK237.IVFAST,
      us_mo_LBAKRDTA_TAK237.IVPLEA,
      us_mo_LBAKRDTA_TAK237.IV_WIT,
      us_mo_LBAKRDTA_TAK237.IVREFU,
      us_mo_LBAKRDTA_TAK237.IVRITE,
      us_mo_LBAKRDTA_TAK237.IVCSQ_,
      us_mo_LBAKRDTA_TAK237.IV_DCR,
      us_mo_LBAKRDTA_TAK237.IV_TCR,
      us_mo_LBAKRDTA_TAK237.IV_DLU,
      us_mo_LBAKRDTA_TAK237.IV_TLU,
      us_mo_LBAKRDTA_TAK237.IV_UID,
      us_mo_LBAKRDTA_TAK237.IV_UIU,
      us_mo_LBAKRDTA_TAK237.file_id,
      us_mo_LBAKRDTA_TAK237.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK237.IV_DOC, us_mo_LBAKRDTA_TAK237.IV_CYC, us_mo_LBAKRDTA_TAK237.IVCSEQ, us_mo_LBAKRDTA_TAK237.IVESEQ]
    note_display: hover
    note_text: "Violations data information (CDV) - Events. Column prefix: IV."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 126
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK291
    title: LBAKRDTA_TAK291
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK291.primary_key,
      us_mo_LBAKRDTA_TAK291.JS_DOC,
      us_mo_LBAKRDTA_TAK291.JS_CYC,
      us_mo_LBAKRDTA_TAK291.JS_CSQ,
      us_mo_LBAKRDTA_TAK291.JS_SEO,
      us_mo_LBAKRDTA_TAK291.JS_FSO,
      us_mo_LBAKRDTA_TAK291.JS_DCR,
      us_mo_LBAKRDTA_TAK291.JS_TCR,
      us_mo_LBAKRDTA_TAK291.JS_UID,
      us_mo_LBAKRDTA_TAK291.JS_DLU,
      us_mo_LBAKRDTA_TAK291.JS_TLU,
      us_mo_LBAKRDTA_TAK291.JS_UIU,
      us_mo_LBAKRDTA_TAK291.file_id,
      us_mo_LBAKRDTA_TAK291.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK291.JS_DOC, us_mo_LBAKRDTA_TAK291.JS_CYC, us_mo_LBAKRDTA_TAK291.JS_CSQ, us_mo_LBAKRDTA_TAK291.JS_SEO]
    note_display: hover
    note_text: "Sentence / Citation Xref. Column prefix: JS."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 132
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_TAK292
    title: LBAKRDTA_TAK292
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_TAK292.primary_key,
      us_mo_LBAKRDTA_TAK292.JT_DOC,
      us_mo_LBAKRDTA_TAK292.JT_CYC,
      us_mo_LBAKRDTA_TAK292.JT_CSQ,
      us_mo_LBAKRDTA_TAK292.JT_TSS,
      us_mo_LBAKRDTA_TAK292.JT_VCV,
      us_mo_LBAKRDTA_TAK292.JT_VG,
      us_mo_LBAKRDTA_TAK292.JT_CCO,
      us_mo_LBAKRDTA_TAK292.JT_DCR,
      us_mo_LBAKRDTA_TAK292.JT_TCR,
      us_mo_LBAKRDTA_TAK292.JT_UID,
      us_mo_LBAKRDTA_TAK292.JT_DLU,
      us_mo_LBAKRDTA_TAK292.JT_TLU,
      us_mo_LBAKRDTA_TAK292.JT_UIU,
      us_mo_LBAKRDTA_TAK292.file_id,
      us_mo_LBAKRDTA_TAK292.is_deleted]
    sorts: [us_mo_LBAKRDTA_TAK292.JT_DOC, us_mo_LBAKRDTA_TAK292.JT_CYC, us_mo_LBAKRDTA_TAK292.JT_CSQ, us_mo_LBAKRDTA_TAK292.JT_TSS]
    note_display: hover
    note_text: "Citations. Column prefix: JT."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 138
    col: 0
    width: 24
    height: 6

  - name: LBAKRDTA_VAK003
    title: LBAKRDTA_VAK003
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_LBAKRDTA_VAK003.primary_key,
      us_mo_LBAKRDTA_VAK003.DOC_ID_DOB,
      us_mo_LBAKRDTA_VAK003.DOB,
      us_mo_LBAKRDTA_VAK003.DOB_VERIFIED_IND,
      us_mo_LBAKRDTA_VAK003.CREATE_DT,
      us_mo_LBAKRDTA_VAK003.CREATE_TM,
      us_mo_LBAKRDTA_VAK003.UPDATE_DT,
      us_mo_LBAKRDTA_VAK003.UPDATE_TM,
      us_mo_LBAKRDTA_VAK003.file_id,
      us_mo_LBAKRDTA_VAK003.is_deleted]
    sorts: [us_mo_LBAKRDTA_VAK003.DOC_ID_DOB]
    note_display: hover
    note_text: "DOB View."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 144
    col: 0
    width: 24
    height: 6

  - name: MASTER_PDB_LOCATIONS
    title: MASTER_PDB_LOCATIONS
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_MASTER_PDB_LOCATIONS.primary_key,
      us_mo_MASTER_PDB_LOCATIONS.LOC_REF_ID,
      us_mo_MASTER_PDB_LOCATIONS.LOC_NM,
      us_mo_MASTER_PDB_LOCATIONS.DIVISION_REF_ID,
      us_mo_MASTER_PDB_LOCATIONS.FUNCTION_TYPE_CD,
      us_mo_MASTER_PDB_LOCATIONS.LOC_ACRONYM,
      us_mo_MASTER_PDB_LOCATIONS.MULES_HIT_PRINTER_ID,
      us_mo_MASTER_PDB_LOCATIONS.ORIGINATING_AGENCY_NO,
      us_mo_MASTER_PDB_LOCATIONS.ADDR_REF_ID,
      us_mo_MASTER_PDB_LOCATIONS.ADDR_OVERRIDE_CD,
      us_mo_MASTER_PDB_LOCATIONS.LOC_START_DT,
      us_mo_MASTER_PDB_LOCATIONS.LOC_STOP_DT,
      us_mo_MASTER_PDB_LOCATIONS.LOC_SORT_SEQ_NO,
      us_mo_MASTER_PDB_LOCATIONS.CREATE_USER_REF_ID,
      us_mo_MASTER_PDB_LOCATIONS.CREATE_TS,
      us_mo_MASTER_PDB_LOCATIONS.UPDATE_USER_REF_ID,
      us_mo_MASTER_PDB_LOCATIONS.UPDATE_TS,
      us_mo_MASTER_PDB_LOCATIONS.file_id,
      us_mo_MASTER_PDB_LOCATIONS.is_deleted]
    sorts: [us_mo_MASTER_PDB_LOCATIONS.LOC_REF_ID]
    note_display: hover
    note_text: "XXX # TODO(#17289): fill in once we have documentation"
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 150
    col: 0
    width: 24
    height: 6

  - name: OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW
    title: OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.primary_key,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.DOC_ID,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.CYCLE_NO,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.OFNDR_CYCLE_REF_ID,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.ACTUAL_START_DT__raw,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.PROJECTED_STOP_DT,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.ACTUAL_STOP_DT__raw,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_TYPE_CD,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_TYPE_DESC,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_COUNTY_CD,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.COUNTY_NM,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.EXIT_TYPE_CD,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.EXIT_TYPE_DESC,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_CD,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_DESC,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_RSN_CD,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_RSN_DESC,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.SUPERVSN_ENH_REF_ID,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.file_id,
      us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.is_deleted]
    sorts: [us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.ACTUAL_START_DT__raw]
    note_display: hover
    note_text: "Supervision Enhancements."
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 156
    col: 0
    width: 24
    height: 6

  - name: OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF
    title: OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF
    explore: us_mo_raw_data
    model: "@{model_name}"
    type: looker_grid
    fields: [us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.primary_key,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.OFNDR_CYCLE_REF_ID,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.DOC_ID,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.CYCLE_NO,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.DELETE_IND,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.CREATE_USER_ID,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.CREATE_TS,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.UPDATE_USER_ID,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.UPDATE_TS,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.CYCLE_CLOSED_IND,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.CREATE_USER_REF_ID,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.file_id,
      us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.is_deleted]
    sorts: [us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.OFNDR_CYCLE_REF_ID]
    note_display: hover
    note_text: "XXX"
    listen: 
      View Type: us_mo_LBAKRDTA_TAK001.view_type
      US_MO_DOC: us_mo_LBAKRDTA_TAK001.EK_DOC
      US_MO_SID: us_mo_LBAKRDTA_TAK001.EK_SID
      US_MO_FBI: us_mo_LBAKRDTA_TAK001.EK_FBI
      US_MO_OLN: us_mo_LBAKRDTA_TAK001.EK_OLN
    row: 162
    col: 0
    width: 24
    height: 6

