# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_tn_raw_data_person_details_staging
  title: Tennessee Raw Data Person Details Staging
  extends: us_tn_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_TN_DOC
    model: recidiviz-staging

  elements:
  - name: OffenderName
    model: recidiviz-staging

  - name: Address
    model: recidiviz-staging

  - name: AssignedStaff
    model: recidiviz-staging

  - name: CAFScore
    model: recidiviz-staging

  - name: CellBedAssignment
    model: recidiviz-staging

  - name: Classification
    model: recidiviz-staging

  - name: ContactNoteType
    model: recidiviz-staging

  - name: Disciplinary
    model: recidiviz-staging

  - name: DisciplinarySentence
    model: recidiviz-staging

  - name: Diversion
    model: recidiviz-staging

  - name: ISCRelatedSentence
    model: recidiviz-staging

  - name: ISCSentence
    model: recidiviz-staging

  - name: JOCharge
    model: recidiviz-staging

  - name: JOIdentification
    model: recidiviz-staging

  - name: JOMiscellaneous
    model: recidiviz-staging

  - name: JOSentence
    model: recidiviz-staging

  - name: JOSpecialConditions
    model: recidiviz-staging

  - name: OffenderAttributes
    model: recidiviz-staging

  - name: OffenderMovement
    model: recidiviz-staging

  - name: Sentence
    model: recidiviz-staging

  - name: SentenceAction
    model: recidiviz-staging

  - name: SentenceMiscellaneous
    model: recidiviz-staging

  - name: SupervisionPlan
    model: recidiviz-staging

  - name: Violations
    model: recidiviz-staging

