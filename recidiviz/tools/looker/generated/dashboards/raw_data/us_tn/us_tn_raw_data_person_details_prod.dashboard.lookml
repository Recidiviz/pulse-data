# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_tn_raw_data_person_details_prod
  title: Tennessee Raw Data Person Details Prod
  extends: us_tn_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_TN_DOC
    model: recidiviz-123

  elements:
  - name: OffenderName
    model: recidiviz-123

  - name: Address
    model: recidiviz-123

  - name: AssignedStaff
    model: recidiviz-123

  - name: CAFScore
    model: recidiviz-123

  - name: CellBedAssignment
    model: recidiviz-123

  - name: Classification
    model: recidiviz-123

  - name: ContactNoteType
    model: recidiviz-123

  - name: Disciplinary
    model: recidiviz-123

  - name: DisciplinarySentence
    model: recidiviz-123

  - name: Diversion
    model: recidiviz-123

  - name: ISCRelatedSentence
    model: recidiviz-123

  - name: ISCSentence
    model: recidiviz-123

  - name: JOCharge
    model: recidiviz-123

  - name: JOIdentification
    model: recidiviz-123

  - name: JOMiscellaneous
    model: recidiviz-123

  - name: JOSentence
    model: recidiviz-123

  - name: JOSpecialConditions
    model: recidiviz-123

  - name: OffenderAttributes
    model: recidiviz-123

  - name: OffenderMovement
    model: recidiviz-123

  - name: Sentence
    model: recidiviz-123

  - name: SentenceAction
    model: recidiviz-123

  - name: SentenceMiscellaneous
    model: recidiviz-123

  - name: SupervisionPlan
    model: recidiviz-123

  - name: Violations
    model: recidiviz-123

