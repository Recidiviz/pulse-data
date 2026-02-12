# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_ix_raw_data_template {
  extension: required

  view_name: us_ix_ind_Offender
  view_label: "us_ix_ind_Offender"

  description: "Data pertaining to an individual in Idaho ATLAS"
  group_label: "Raw State Data"
  label: "US_IX Raw Data"
  join: us_ix_asm_Assessment {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_asm_Assessment.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_asm_Assessment"
  }

  join: us_ix_com_CommunityServiceRecord {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_com_CommunityServiceRecord.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_com_CommunityServiceRecord"
  }

  join: us_ix_com_Investigation {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_com_Investigation.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_com_Investigation"
  }

  join: us_ix_com_PSIReport {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_com_PSIReport.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_com_PSIReport"
  }

  join: us_ix_com_PhysicalLocation {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_com_PhysicalLocation.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_com_PhysicalLocation"
  }

  join: us_ix_com_Transfer {
    sql_on: ${us_ix_com_Transfer.OffenderId} = ${us_ix_ind_Offender.OffenderId};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_ix_com_Transfer"
  }

  join: us_ix_crs_OfdCourseEnrollment {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_crs_OfdCourseEnrollment.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_crs_OfdCourseEnrollment"
  }

  join: us_ix_drg_DrugTestResult {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_drg_DrugTestResult.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_drg_DrugTestResult"
  }

  join: us_ix_dsc_DACase {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_dsc_DACase.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_dsc_DACase"
  }

  join: us_ix_dsc_DAProcedure {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_dsc_DAProcedure.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_dsc_DAProcedure"
  }

  join: us_ix_fin_AccountOwner {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_fin_AccountOwner.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_fin_AccountOwner"
  }

  join: us_ix_gsm_ParticipantOffender {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_gsm_ParticipantOffender.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_gsm_ParticipantOffender"
  }

  join: us_ix_hsn_BedAssignment {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_hsn_BedAssignment.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_hsn_BedAssignment"
  }

  join: us_ix_ind_AliasName {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_AliasName.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_AliasName"
  }

  join: us_ix_ind_EmploymentHistory {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_EmploymentHistory.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_EmploymentHistory"
  }

  join: us_ix_ind_OffenderInternalStatus {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_OffenderInternalStatus.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_OffenderInternalStatus"
  }

  join: us_ix_ind_OffenderNoteInfo {
    sql_on: ${us_ix_ind_OffenderNoteInfo.OffenderId} = ${us_ix_ind_Offender.OffenderId};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_ix_ind_OffenderNoteInfo"
  }

  join: us_ix_ind_OffenderSecurityLevel {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_OffenderSecurityLevel.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_OffenderSecurityLevel"
  }

  join: us_ix_ind_Offender_Address {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_Offender_Address.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_Offender_Address"
  }

  join: us_ix_ind_Offender_Alert {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_Offender_Alert.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_Offender_Alert"
  }

  join: us_ix_ind_Offender_EmailAddress {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_Offender_EmailAddress.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_Offender_EmailAddress"
  }

  join: us_ix_ind_Offender_Phone {
    sql_on: ${us_ix_ind_Offender_Phone.OffenderId} = ${us_ix_ind_Offender.OffenderId};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_ix_ind_Offender_Phone"
  }

  join: us_ix_ind_Offender_QuestionnaireTemplate {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_ind_Offender_QuestionnaireTemplate.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_ind_Offender_QuestionnaireTemplate"
  }

  join: us_ix_movement {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_movement.docno};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_movement"
  }

  join: us_ix_prb_PBCase {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_prb_PBCase.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_prb_PBCase"
  }

  join: us_ix_scb_OffenderJcbInternalStatus {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scb_OffenderJcbInternalStatus.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scb_OffenderJcbInternalStatus"
  }

  join: us_ix_scl_Charge {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Charge.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Charge"
  }

  join: us_ix_scl_DiscOffenseRpt {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_DiscOffenseRpt.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_DiscOffenseRpt"
  }

  join: us_ix_scl_Escape {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Escape.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Escape"
  }

  join: us_ix_scl_MasterTerm {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_MasterTerm.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_MasterTerm"
  }

  join: us_ix_scl_Parole {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Parole.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Parole"
  }

  join: us_ix_scl_Sentence {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Sentence.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Sentence"
  }

  join: us_ix_scl_SentenceOrder {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_SentenceOrder.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_SentenceOrder"
  }

  join: us_ix_scl_Supervision {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Supervision.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Supervision"
  }

  join: us_ix_scl_Term {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Term.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Term"
  }

  join: us_ix_scl_Violation {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Violation.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Violation"
  }

  join: us_ix_scl_Warrant {
    sql_on: ${us_ix_ind_Offender.OffenderId} = ${us_ix_scl_Warrant.OffenderId};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ix_scl_Warrant"
  }

}
