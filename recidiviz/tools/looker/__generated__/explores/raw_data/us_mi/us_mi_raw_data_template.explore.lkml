# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_mi_raw_data_template {
  extension: required

  view_name: us_mi_ADH_OFFENDER
  view_label: "us_mi_ADH_OFFENDER"

  description: "Data pertaining to an individual in Michigan"
  group_label: "Raw State Data"
  label: "US_MI Raw Data"
  join: us_mi_ADH_FACILITY_COUNT_SHEET {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_ADH_FACILITY_COUNT_SHEET.offender_number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_FACILITY_COUNT_SHEET"
  }

  join: us_mi_ADH_FACILITY_COUNT_SHEET_HIST {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_ADH_FACILITY_COUNT_SHEET_HIST.offender_number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_FACILITY_COUNT_SHEET_HIST"
  }

  join: us_mi_ADH_OFFENDER_BOOKING {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_BOOKING.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_BOOKING"
  }

  join: us_mi_ADH_OFFENDER_DESIGNATION {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_DESIGNATION.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_DESIGNATION"
  }

  join: us_mi_ADH_OFFENDER_DETAINER {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_DETAINER.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_DETAINER"
  }

  join: us_mi_ADH_OFFENDER_ERD {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_ERD.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_ERD"
  }

  join: us_mi_ADH_OFFENDER_LOCK {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_LOCK.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_LOCK"
  }

  join: us_mi_ADH_OFFENDER_NAME {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_NAME.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_NAME"
  }

  join: us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.offender_number} AND ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_PROFILE_SUMMARY_WRK"
  }

  join: us_mi_ADH_OFFENDER_SENTENCE {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_OFFENDER_SENTENCE.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_SENTENCE"
  }

  join: us_mi_ADH_PERSON {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_PERSON.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_PERSON"
  }

  join: us_mi_ADH_PERSONAL_PROTECTION_ORDER {
    sql_on: ${us_mi_ADH_OFFENDER.offender_id} = ${us_mi_ADH_PERSONAL_PROTECTION_ORDER.offender_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_PERSONAL_PROTECTION_ORDER"
  }

  join: us_mi_ADH_SHOFFENDER {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_ADH_SHOFFENDER.OffenderNumber};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_SHOFFENDER"
  }

  join: us_mi_COMS_Assaultive_Risk_Assessments {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Assaultive_Risk_Assessments.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Assaultive_Risk_Assessments"
  }

  join: us_mi_COMS_Case_Managers {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Case_Managers.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Case_Managers"
  }

  join: us_mi_COMS_Case_Notes {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Case_Notes.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Case_Notes"
  }

  join: us_mi_COMS_Employment {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Employment.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Employment"
  }

  join: us_mi_COMS_Modifiers {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Modifiers.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Modifiers"
  }

  join: us_mi_COMS_Parole_Violation_Violation_Incidents {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Parole_Violation_Violation_Incidents.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Parole_Violation_Violation_Incidents"
  }

  join: us_mi_COMS_Parole_Violations {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Parole_Violations.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Parole_Violations"
  }

  join: us_mi_COMS_Probation_Violation_Violation_Incidents {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Probation_Violation_Violation_Incidents.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Probation_Violation_Violation_Incidents"
  }

  join: us_mi_COMS_Probation_Violations {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Probation_Violations.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Probation_Violations"
  }

  join: us_mi_COMS_Program_Recommendations {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Program_Recommendations.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Program_Recommendations"
  }

  join: us_mi_COMS_Security_Classification {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Security_Classification.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Security_Classification"
  }

  join: us_mi_COMS_Security_Standards_Toxin {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Security_Standards_Toxin.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Security_Standards_Toxin"
  }

  join: us_mi_COMS_Security_Threat_Group_Involvement {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Security_Threat_Group_Involvement.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Security_Threat_Group_Involvement"
  }

  join: us_mi_COMS_Specialties {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Specialties.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Specialties"
  }

  join: us_mi_COMS_Supervision_Levels {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Supervision_Levels.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Supervision_Levels"
  }

  join: us_mi_COMS_Supervision_Schedule_Activities {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Supervision_Schedule_Activities.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Supervision_Schedule_Activities"
  }

  join: us_mi_COMS_Supervision_Schedules {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Supervision_Schedules.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Supervision_Schedules"
  }

  join: us_mi_COMS_Violation_Incident_Charges {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Violation_Incident_Charges.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Violation_Incident_Charges"
  }

  join: us_mi_COMS_Violation_Incidents {
    sql_on: ${us_mi_ADH_OFFENDER.offender_number} = ${us_mi_COMS_Violation_Incidents.Offender_Number};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_COMS_Violation_Incidents"
  }

  join: us_mi_ADH_CASE_NOTE_DETAIL {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_CASE_NOTE_DETAIL.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_CASE_NOTE_DETAIL"
  }

  join: us_mi_ADH_EMC_WARRANT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_EMC_WARRANT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_EMC_WARRANT"
  }

  join: us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_EMPLOYEE_BOOKING_ASSIGNMENT"
  }

  join: us_mi_ADH_LEGAL_ORDER {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_LEGAL_ORDER.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_LEGAL_ORDER"
  }

  join: us_mi_ADH_MISCONDUCT_INCIDENT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_MISCONDUCT_INCIDENT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_MISCONDUCT_INCIDENT"
  }

  join: us_mi_ADH_OFFENDER_ASSESSMENT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_ASSESSMENT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_ASSESSMENT"
  }

  join: us_mi_ADH_OFFENDER_BASIC_INFO_104A {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_BASIC_INFO_104A.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_BASIC_INFO_104A"
  }

  join: us_mi_ADH_OFFENDER_BOOKING_PROFILE {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_BOOKING_PROFILE.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_BOOKING_PROFILE"
  }

  join: us_mi_ADH_OFFENDER_BOOKING_REPORT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_BOOKING_REPORT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_BOOKING_REPORT"
  }

  join: us_mi_ADH_OFFENDER_CHARGE {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_CHARGE.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_CHARGE"
  }

  join: us_mi_ADH_OFFENDER_EMPLOYMENT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_EMPLOYMENT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_EMPLOYMENT"
  }

  join: us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_EXTERNAL_MOVEMENT"
  }

  join: us_mi_ADH_OFFENDER_FEE_PROFILE {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_FEE_PROFILE.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_FEE_PROFILE"
  }

  join: us_mi_ADH_OFFENDER_MISCOND_HEARING {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_MISCOND_HEARING.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_MISCOND_HEARING"
  }

  join: us_mi_ADH_OFFENDER_RGC_RECOMMENDATION {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_RGC_RECOMMENDATION.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_RGC_RECOMMENDATION"
  }

  join: us_mi_ADH_OFFENDER_RGC_TRACKING {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_RGC_TRACKING.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_RGC_TRACKING"
  }

  join: us_mi_ADH_OFFENDER_SCHEDULE {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_SCHEDULE.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_SCHEDULE"
  }

  join: us_mi_ADH_OFFENDER_SUPERVISION {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_OFFENDER_SUPERVISION.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_OFFENDER_SUPERVISION"
  }

  join: us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_PERSONAL_PROTECTION_ORDER_NOTE"
  }

  join: us_mi_ADH_PLAN_OF_SUPERVISION {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_PLAN_OF_SUPERVISION.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_PLAN_OF_SUPERVISION"
  }

  join: us_mi_ADH_SUBSTANCE_ABUSE_TEST {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_SUBSTANCE_ABUSE_TEST.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_SUBSTANCE_ABUSE_TEST"
  }

  join: us_mi_ADH_SUPERVISION_CONDITION {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_SUPERVISION_CONDITION.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_SUPERVISION_CONDITION"
  }

  join: us_mi_ADH_SUPERVISION_VIOLATION {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_SUPERVISION_VIOLATION.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_SUPERVISION_VIOLATION"
  }

  join: us_mi_ADH_SUPER_COND_VIOLATION {
    sql_on: ${us_mi_ADH_OFFENDER_BOOKING.offender_booking_id} = ${us_mi_ADH_SUPER_COND_VIOLATION.offender_booking_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_mi_ADH_SUPER_COND_VIOLATION"
  }

}
