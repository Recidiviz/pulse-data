# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_mo_raw_data_template {
  extension: required

  view_name: us_mo_LBAKRDTA_TAK001
  view_label: "us_mo_LBAKRDTA_TAK001"

  description: "Data pertaining to an individual in Missouri"
  group_label: "Raw State Data"
  label: "US_MO Raw Data"
  join: us_mo_ORAS_WEEKLY_SUMMARY_UPDATE {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_ORAS_WEEKLY_SUMMARY_UPDATE.DOC_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_ORAS_WEEKLY_SUMMARY_UPDATE"
  }

  join: us_mo_LBAKRDTA_TAK015 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK015.BL_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK015"
  }

  join: us_mo_LBAKRDTA_TAK017 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK017.BN_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK017"
  }

  join: us_mo_LBAKRDTA_TAK020 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK020.BQ_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK020"
  }

  join: us_mo_LBAKRDTA_TAK022 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK022.BS_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK022"
  }

  join: us_mo_LBAKRDTA_TAK023 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK023.BT_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK023"
  }

  join: us_mo_LBAKRDTA_TAK024 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK024.BU_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK024"
  }

  join: us_mo_LBAKRDTA_TAK025 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK025.BV_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK025"
  }

  join: us_mo_LBAKRDTA_TAK026 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK026.BW_DOC};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_mo_LBAKRDTA_TAK026"
  }

  join: us_mo_LBAKRDTA_TAK028 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK028.BY_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK028"
  }

  join: us_mo_LBAKRDTA_TAK034 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK034.CE_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK034"
  }

  join: us_mo_LBAKRDTA_TAK039 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK039.DN_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK039"
  }

  join: us_mo_LBAKRDTA_TAK042 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK042.CF_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK042"
  }

  join: us_mo_LBAKRDTA_TAK044 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK044.CG_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK044"
  }

  join: us_mo_LBAKRDTA_TAK065 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK065.CS_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK065"
  }

  join: us_mo_LBAKRDTA_TAK076 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK076.CZ_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK076"
  }

  join: us_mo_LBAKRDTA_TAK142 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK142.E6_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK142"
  }

  join: us_mo_LBAKRDTA_TAK158 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK158.F1_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK158"
  }

  join: us_mo_LBAKRDTA_TAK233 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK233.IZ_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK233"
  }

  join: us_mo_LBAKRDTA_TAK236 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK236.IU_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK236"
  }

  join: us_mo_LBAKRDTA_TAK237 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK237.IV_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK237"
  }

  join: us_mo_LBAKRDTA_TAK291 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK291.JS_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK291"
  }

  join: us_mo_LBAKRDTA_TAK292 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_TAK292.JT_DOC};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_TAK292"
  }

  join: us_mo_LBAKRDTA_VAK003 {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_LBAKRDTA_VAK003.DOC_ID_DOB};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_LBAKRDTA_VAK003"
  }

  join: us_mo_MASTER_PDB_LOCATIONS {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_OLC} = ${us_mo_MASTER_PDB_LOCATIONS.LOC_REF_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_MASTER_PDB_LOCATIONS"
  }

  join: us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW.DOC_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW"
  }

  join: us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF {
    sql_on: ${us_mo_LBAKRDTA_TAK001.EK_DOC} = ${us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF.DOC_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mo_OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF"
  }

}
