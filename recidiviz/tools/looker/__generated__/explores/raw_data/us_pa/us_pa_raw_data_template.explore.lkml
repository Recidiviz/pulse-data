# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_pa_raw_data_template {
  extension: required

  view_name: us_pa_dbo_tblSearchInmateInfo
  view_label: "us_pa_dbo_tblSearchInmateInfo"

  description: "Data pertaining to an individual in Pennsylvania"
  group_label: "Raw State Data"
  label: "US_PA Raw Data"
  join: us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.control_number} = ${us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids.control_number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_RECIDIVIZ_REFERENCE_control_number_linking_ids"
  }

  join: us_pa_dbo_BoardAction {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_BoardAction.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_BoardAction"
  }

  join: us_pa_dbo_ConditionCode {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_ConditionCode.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_ConditionCode"
  }

  join: us_pa_dbo_ConditionCodeDescription {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_ConditionCodeDescription.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_ConditionCodeDescription"
  }

  join: us_pa_dbo_DOB {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_DOB.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_DOB"
  }

  join: us_pa_dbo_DeActivateParoleNumber {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_DeActivateParoleNumber.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_DeActivateParoleNumber"
  }

  join: us_pa_dbo_Hist_Parolee {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Hist_Parolee.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Hist_Parolee"
  }

  join: us_pa_dbo_Hist_Release {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Hist_Release.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Hist_Release"
  }

  join: us_pa_dbo_Hist_SanctionTracking {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Hist_SanctionTracking.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Hist_SanctionTracking"
  }

  join: us_pa_dbo_Hist_Treatment {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Hist_Treatment.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Hist_Treatment"
  }

  join: us_pa_dbo_LSIHistory {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_LSIHistory.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_LSIHistory"
  }

  join: us_pa_dbo_LSIR {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_LSIR.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_LSIR"
  }

  join: us_pa_dbo_Miscon {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.control_number} = ${us_pa_dbo_Miscon.control_number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Miscon"
  }

  join: us_pa_dbo_Movrec {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.inmate_number} = ${us_pa_dbo_Movrec.mov_cur_inmt_num} AND ${us_pa_dbo_tblSearchInmateInfo.control_number} = ${us_pa_dbo_Movrec.mov_cnt_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Movrec"
  }

  join: us_pa_dbo_Offender {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Offender.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Offender"
  }

  join: us_pa_dbo_ParoleCount {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_ParoleCount.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_ParoleCount"
  }

  join: us_pa_dbo_Parolee {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Parolee.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Parolee"
  }

  join: us_pa_dbo_Perrec {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Perrec.parole_board_num} AND ${us_pa_dbo_tblSearchInmateInfo.control_number} = ${us_pa_dbo_Perrec.control_number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Perrec"
  }

  join: us_pa_dbo_RelAgentHistory {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_RelAgentHistory.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_RelAgentHistory"
  }

  join: us_pa_dbo_RelEmployment {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_RelEmployment.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_RelEmployment"
  }

  join: us_pa_dbo_Release {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Release.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Release"
  }

  join: us_pa_dbo_ReleaseInfo {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_ReleaseInfo.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_ReleaseInfo"
  }

  join: us_pa_dbo_SanctionTracking {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_SanctionTracking.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_SanctionTracking"
  }

  join: us_pa_dbo_Senrec {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.inmate_number} = ${us_pa_dbo_Senrec.curr_inmate_num};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Senrec"
  }

  join: us_pa_dbo_Sentence {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Sentence.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Sentence"
  }

  join: us_pa_dbo_SentenceGroup {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_SentenceGroup.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_SentenceGroup"
  }

  join: us_pa_dbo_Treatment {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.parole_board_num} = ${us_pa_dbo_Treatment.ParoleNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_Treatment"
  }

  join: us_pa_dbo_tblInmTestScore {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.inmate_number} = ${us_pa_dbo_tblInmTestScore.Inmate_number} AND ${us_pa_dbo_tblSearchInmateInfo.control_number} = ${us_pa_dbo_tblInmTestScore.Control_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_tblInmTestScore"
  }

  join: us_pa_dbo_tblInmTestScoreHist {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.inmate_number} = ${us_pa_dbo_tblInmTestScoreHist.Inmate_number} AND ${us_pa_dbo_tblSearchInmateInfo.control_number} = ${us_pa_dbo_tblInmTestScoreHist.Control_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_tblInmTestScoreHist"
  }

  join: us_pa_dbo_vwCCISAllMvmt {
    sql_on: ${us_pa_dbo_tblSearchInmateInfo.inmate_number} = ${us_pa_dbo_vwCCISAllMvmt.Inmate_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_pa_dbo_vwCCISAllMvmt"
  }

}
