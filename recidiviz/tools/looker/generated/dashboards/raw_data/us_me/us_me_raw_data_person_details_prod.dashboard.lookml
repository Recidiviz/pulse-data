# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_me_raw_data_person_details_prod
  title: Maine Raw Data Person Details Prod
  extends: us_me_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_ME_DOC
    model: recidiviz-123

  elements:
  - name: CIS_100_CLIENT
    model: recidiviz-123

  - name: CIS_102_ALERT_HISTORY
    model: recidiviz-123

  - name: CIS_106_ADDRESS
    model: recidiviz-123

  - name: CIS_112_CUSTODY_LEVEL
    model: recidiviz-123

  - name: CIS_116_LSI_HISTORY
    model: recidiviz-123

  - name: CIS_124_SUPERVISION_HISTORY
    model: recidiviz-123

  - name: CIS_125_CURRENT_STATUS_HIST
    model: recidiviz-123

  - name: CIS_128_EMPLOYMENT_HISTORY
    model: recidiviz-123

  - name: CIS_130_INVESTIGATION
    model: recidiviz-123

  - name: CIS_140_CLASSIFICATION_REVIEW
    model: recidiviz-123

  - name: CIS_160_DRUG_SCREENING
    model: recidiviz-123

  - name: CIS_200_CASE_PLAN
    model: recidiviz-123

  - name: CIS_201_GOALS
    model: recidiviz-123

  - name: CIS_204_GEN_NOTE
    model: recidiviz-123

  - name: CIS_210_JOB_ASSIGN
    model: recidiviz-123

  - name: CIS_215_SEX_OFFENDER_ASSESS
    model: recidiviz-123

  - name: CIS_300_Personal_Property
    model: recidiviz-123

  - name: CIS_309_MOVEMENT
    model: recidiviz-123

  - name: CIS_314_TRANSFER
    model: recidiviz-123

  - name: CIS_319_TERM
    model: recidiviz-123

  - name: CIS_324_AWOL
    model: recidiviz-123

  - name: CIS_400_CHARGE
    model: recidiviz-123

  - name: CIS_401_CRT_ORDER_HDR
    model: recidiviz-123

  - name: CIS_425_MAIN_PROG
    model: recidiviz-123

  - name: CIS_430_ASSESSMENT
    model: recidiviz-123

  - name: CIS_462_CLIENTS_INVOLVED
    model: recidiviz-123

  - name: CIS_480_VIOLATION
    model: recidiviz-123

  - name: CIS_573_CLIENT_CASE_DETAIL
    model: recidiviz-123

  - name: CIS_916_ASSIGN_BED
    model: recidiviz-123

