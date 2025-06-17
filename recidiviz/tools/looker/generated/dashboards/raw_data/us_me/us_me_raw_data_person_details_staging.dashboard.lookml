# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_me_raw_data_person_details_staging
  title: Maine Raw Data Person Details Staging
  extends: us_me_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_ME_DOC
    model: recidiviz-staging

  elements:
  - name: CIS_100_CLIENT
    model: recidiviz-staging

  - name: CIS_102_ALERT_HISTORY
    model: recidiviz-staging

  - name: CIS_106_ADDRESS
    model: recidiviz-staging

  - name: CIS_112_CUSTODY_LEVEL
    model: recidiviz-staging

  - name: CIS_116_LSI_HISTORY
    model: recidiviz-staging

  - name: CIS_124_SUPERVISION_HISTORY
    model: recidiviz-staging

  - name: CIS_125_CURRENT_STATUS_HIST
    model: recidiviz-staging

  - name: CIS_128_EMPLOYMENT_HISTORY
    model: recidiviz-staging

  - name: CIS_130_INVESTIGATION
    model: recidiviz-staging

  - name: CIS_140_CLASSIFICATION_REVIEW
    model: recidiviz-staging

  - name: CIS_160_DRUG_SCREENING
    model: recidiviz-staging

  - name: CIS_200_CASE_PLAN
    model: recidiviz-staging

  - name: CIS_201_GOALS
    model: recidiviz-staging

  - name: CIS_204_GEN_NOTE
    model: recidiviz-staging

  - name: CIS_210_JOB_ASSIGN
    model: recidiviz-staging

  - name: CIS_215_SEX_OFFENDER_ASSESS
    model: recidiviz-staging

  - name: CIS_300_Personal_Property
    model: recidiviz-staging

  - name: CIS_309_MOVEMENT
    model: recidiviz-staging

  - name: CIS_314_TRANSFER
    model: recidiviz-staging

  - name: CIS_319_TERM
    model: recidiviz-staging

  - name: CIS_324_AWOL
    model: recidiviz-staging

  - name: CIS_400_CHARGE
    model: recidiviz-staging

  - name: CIS_401_CRT_ORDER_HDR
    model: recidiviz-staging

  - name: CIS_425_MAIN_PROG
    model: recidiviz-staging

  - name: CIS_430_ASSESSMENT
    model: recidiviz-staging

  - name: CIS_462_CLIENTS_INVOLVED
    model: recidiviz-staging

  - name: CIS_480_VIOLATION
    model: recidiviz-staging

  - name: CIS_573_CLIENT_CASE_DETAIL
    model: recidiviz-staging

  - name: CIS_916_ASSIGN_BED
    model: recidiviz-staging

