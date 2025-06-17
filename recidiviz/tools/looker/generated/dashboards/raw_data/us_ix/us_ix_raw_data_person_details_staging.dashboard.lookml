# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ix_raw_data_person_details_staging
  title: Idaho ATLAS Raw Data Person Details Staging
  extends: us_ix_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_IX_DOC
    model: recidiviz-staging

  elements:
  - name: ind_Offender
    model: recidiviz-staging

  - name: asm_Assessment
    model: recidiviz-staging

  - name: com_CommunityServiceRecord
    model: recidiviz-staging

  - name: com_PSIReport
    model: recidiviz-staging

  - name: com_PhysicalLocation
    model: recidiviz-staging

  - name: com_Transfer
    model: recidiviz-staging

  - name: crs_OfdCourseEnrollment
    model: recidiviz-staging

  - name: drg_DrugTestResult
    model: recidiviz-staging

  - name: dsc_DACase
    model: recidiviz-staging

  - name: dsc_DAProcedure
    model: recidiviz-staging

  - name: hsn_BedAssignment
    model: recidiviz-staging

  - name: ind_AliasName
    model: recidiviz-staging

  - name: ind_EmploymentHistory
    model: recidiviz-staging

  - name: ind_OffenderNoteInfo
    model: recidiviz-staging

  - name: ind_OffenderSecurityLevel
    model: recidiviz-staging

  - name: ind_Offender_Address
    model: recidiviz-staging

  - name: ind_Offender_Alert
    model: recidiviz-staging

  - name: ind_Offender_EmailAddress
    model: recidiviz-staging

  - name: ind_Offender_Phone
    model: recidiviz-staging

  - name: ind_Offender_QuestionnaireTemplate
    model: recidiviz-staging

  - name: prb_PBCase
    model: recidiviz-staging

  - name: scl_Charge
    model: recidiviz-staging

  - name: scl_DiscOffenseRpt
    model: recidiviz-staging

  - name: scl_Escape
    model: recidiviz-staging

  - name: scl_MasterTerm
    model: recidiviz-staging

  - name: scl_Parole
    model: recidiviz-staging

  - name: scl_Sentence
    model: recidiviz-staging

  - name: scl_SentenceOrder
    model: recidiviz-staging

  - name: scl_Supervision
    model: recidiviz-staging

  - name: scl_Term
    model: recidiviz-staging

  - name: scl_Violation
    model: recidiviz-staging

  - name: scl_Warrant
    model: recidiviz-staging

