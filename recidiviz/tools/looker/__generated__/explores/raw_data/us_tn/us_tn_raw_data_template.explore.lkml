# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_tn_raw_data_template {
  extension: required

  view_name: us_tn_OffenderName
  view_label: "us_tn_OffenderName"

  description: "Data pertaining to an individual in Tennessee"
  group_label: "Raw State Data"
  label: "US_TN Raw Data"
  join: us_tn_Address {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Address.PersonID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Address"
  }

  join: us_tn_AssignedStaff {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_AssignedStaff.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_AssignedStaff"
  }

  join: us_tn_CAFScore {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_CAFScore.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_CAFScore"
  }

  join: us_tn_CellBedAssignment {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_CellBedAssignment.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_CellBedAssignment"
  }

  join: us_tn_Classification {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Classification.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Classification"
  }

  join: us_tn_ContactNoteType {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_ContactNoteType.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_ContactNoteType"
  }

  join: us_tn_Disciplinary {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Disciplinary.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Disciplinary"
  }

  join: us_tn_DisciplinarySentence {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_DisciplinarySentence.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_DisciplinarySentence"
  }

  join: us_tn_Diversion {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Diversion.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Diversion"
  }

  join: us_tn_ISCRelatedSentence {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_ISCRelatedSentence.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_ISCRelatedSentence"
  }

  join: us_tn_ISCSentence {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_ISCSentence.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_ISCSentence"
  }

  join: us_tn_JOCharge {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_JOCharge.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_JOCharge"
  }

  join: us_tn_JOIdentification {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_JOIdentification.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_JOIdentification"
  }

  join: us_tn_JOMiscellaneous {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_JOMiscellaneous.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_JOMiscellaneous"
  }

  join: us_tn_JOSentence {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_JOSentence.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_JOSentence"
  }

  join: us_tn_JOSpecialConditions {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_JOSpecialConditions.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_JOSpecialConditions"
  }

  join: us_tn_Offender {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Offender.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Offender"
  }

  join: us_tn_OffenderAttributes {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_OffenderAttributes.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_OffenderAttributes"
  }

  join: us_tn_OffenderMovement {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_OffenderMovement.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_OffenderMovement"
  }

  join: us_tn_Sentence {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Sentence.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Sentence"
  }

  join: us_tn_SentenceAction {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_SentenceAction.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_SentenceAction"
  }

  join: us_tn_SentenceMiscellaneous {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_SentenceMiscellaneous.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_SentenceMiscellaneous"
  }

  join: us_tn_SupervisionPlan {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_SupervisionPlan.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_SupervisionPlan"
  }

  join: us_tn_Violations {
    sql_on: ${us_tn_OffenderName.OffenderID} = ${us_tn_Violations.OffenderID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_tn_Violations"
  }

}
