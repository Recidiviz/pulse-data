# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ix_raw_data_person_details_prod
  title: Idaho ATLAS Raw Data Person Details Prod
  extends: us_ix_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_IX_DOC
    model: recidiviz-123

  elements:
  - name: ind_Offender
    model: recidiviz-123

  - name: asm_Assessment
    model: recidiviz-123

  - name: com_CommunityServiceRecord
    model: recidiviz-123

  - name: com_PSIReport
    model: recidiviz-123

  - name: com_PhysicalLocation
    model: recidiviz-123

  - name: com_Transfer
    model: recidiviz-123

  - name: crs_OfdCourseEnrollment
    model: recidiviz-123

  - name: drg_DrugTestResult
    model: recidiviz-123

  - name: dsc_DACase
    model: recidiviz-123

  - name: dsc_DAProcedure
    model: recidiviz-123

  - name: hsn_BedAssignment
    model: recidiviz-123

  - name: ind_AliasName
    model: recidiviz-123

  - name: ind_EmploymentHistory
    model: recidiviz-123

  - name: ind_OffenderNoteInfo
    model: recidiviz-123

  - name: ind_OffenderSecurityLevel
    model: recidiviz-123

  - name: ind_Offender_Address
    model: recidiviz-123

  - name: ind_Offender_Alert
    model: recidiviz-123

  - name: ind_Offender_EmailAddress
    model: recidiviz-123

  - name: ind_Offender_Phone
    model: recidiviz-123

  - name: ind_Offender_QuestionnaireTemplate
    model: recidiviz-123

  - name: prb_PBCase
    model: recidiviz-123

  - name: scl_Charge
    model: recidiviz-123

  - name: scl_DiscOffenseRpt
    model: recidiviz-123

  - name: scl_Escape
    model: recidiviz-123

  - name: scl_MasterTerm
    model: recidiviz-123

  - name: scl_Parole
    model: recidiviz-123

  - name: scl_Sentence
    model: recidiviz-123

  - name: scl_SentenceOrder
    model: recidiviz-123

  - name: scl_Supervision
    model: recidiviz-123

  - name: scl_Term
    model: recidiviz-123

  - name: scl_Violation
    model: recidiviz-123

  - name: scl_Warrant
    model: recidiviz-123

