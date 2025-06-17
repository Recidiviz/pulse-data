# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_mi_raw_data_person_details_staging
  title: Michigan Raw Data Person Details Staging
  extends: us_mi_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_MI_DOC_ID
    model: recidiviz-staging

  - name: US_MI_DOC
    model: recidiviz-staging

  - name: US_MI_DOC_BOOK
    model: recidiviz-staging

  elements:
  - name: ADH_OFFENDER
    model: recidiviz-staging

  - name: ADH_FACILITY_COUNT_SHEET
    model: recidiviz-staging

  - name: ADH_FACILITY_COUNT_SHEET_HIST
    model: recidiviz-staging

  - name: ADH_OFFENDER_BOOKING
    model: recidiviz-staging

  - name: ADH_OFFENDER_DESIGNATION
    model: recidiviz-staging

  - name: ADH_OFFENDER_DETAINER
    model: recidiviz-staging

  - name: ADH_OFFENDER_ERD
    model: recidiviz-staging

  - name: ADH_OFFENDER_LOCK
    model: recidiviz-staging

  - name: ADH_OFFENDER_NAME
    model: recidiviz-staging

  - name: ADH_OFFENDER_PROFILE_SUMMARY_WRK
    model: recidiviz-staging

  - name: ADH_OFFENDER_SENTENCE
    model: recidiviz-staging

  - name: ADH_PERSON
    model: recidiviz-staging

  - name: ADH_PERSONAL_PROTECTION_ORDER
    model: recidiviz-staging

  - name: ADH_SHOFFENDER
    model: recidiviz-staging

  - name: COMS_Assaultive_Risk_Assessments
    model: recidiviz-staging

  - name: COMS_Case_Managers
    model: recidiviz-staging

  - name: COMS_Case_Notes
    model: recidiviz-staging

  - name: COMS_Employment
    model: recidiviz-staging

  - name: COMS_Modifiers
    model: recidiviz-staging

  - name: COMS_Parole_Violation_Violation_Incidents
    model: recidiviz-staging

  - name: COMS_Parole_Violations
    model: recidiviz-staging

  - name: COMS_Probation_Violation_Violation_Incidents
    model: recidiviz-staging

  - name: COMS_Probation_Violations
    model: recidiviz-staging

  - name: COMS_Program_Recommendations
    model: recidiviz-staging

  - name: COMS_Security_Classification
    model: recidiviz-staging

  - name: COMS_Security_Standards_Toxin
    model: recidiviz-staging

  - name: COMS_Security_Threat_Group_Involvement
    model: recidiviz-staging

  - name: COMS_Specialties
    model: recidiviz-staging

  - name: COMS_Supervision_Levels
    model: recidiviz-staging

  - name: COMS_Supervision_Schedule_Activities
    model: recidiviz-staging

  - name: COMS_Supervision_Schedules
    model: recidiviz-staging

  - name: COMS_Violation_Incident_Charges
    model: recidiviz-staging

  - name: COMS_Violation_Incidents
    model: recidiviz-staging

  - name: ADH_CASE_NOTE_DETAIL
    model: recidiviz-staging

  - name: ADH_EMC_WARRANT
    model: recidiviz-staging

  - name: ADH_EMPLOYEE_BOOKING_ASSIGNMENT
    model: recidiviz-staging

  - name: ADH_LEGAL_ORDER
    model: recidiviz-staging

  - name: ADH_MISCONDUCT_INCIDENT
    model: recidiviz-staging

  - name: ADH_OFFENDER_ASSESSMENT
    model: recidiviz-staging

  - name: ADH_OFFENDER_BASIC_INFO_104A
    model: recidiviz-staging

  - name: ADH_OFFENDER_BOOKING_PROFILE
    model: recidiviz-staging

  - name: ADH_OFFENDER_BOOKING_REPORT
    model: recidiviz-staging

  - name: ADH_OFFENDER_CHARGE
    model: recidiviz-staging

  - name: ADH_OFFENDER_EMPLOYMENT
    model: recidiviz-staging

  - name: ADH_OFFENDER_EXTERNAL_MOVEMENT
    model: recidiviz-staging

  - name: ADH_OFFENDER_FEE_PROFILE
    model: recidiviz-staging

  - name: ADH_OFFENDER_MISCOND_HEARING
    model: recidiviz-staging

  - name: ADH_OFFENDER_RGC_RECOMMENDATION
    model: recidiviz-staging

  - name: ADH_OFFENDER_RGC_TRACKING
    model: recidiviz-staging

  - name: ADH_OFFENDER_SCHEDULE
    model: recidiviz-staging

  - name: ADH_OFFENDER_SUPERVISION
    model: recidiviz-staging

  - name: ADH_PERSONAL_PROTECTION_ORDER_NOTE
    model: recidiviz-staging

  - name: ADH_PLAN_OF_SUPERVISION
    model: recidiviz-staging

  - name: ADH_SUBSTANCE_ABUSE_TEST
    model: recidiviz-staging

  - name: ADH_SUPERVISION_CONDITION
    model: recidiviz-staging

  - name: ADH_SUPERVISION_VIOLATION
    model: recidiviz-staging

  - name: ADH_SUPER_COND_VIOLATION
    model: recidiviz-staging

