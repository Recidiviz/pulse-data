# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_mo_raw_data_person_details_staging
  title: Missouri Raw Data Person Details Staging
  extends: us_mo_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_MO_DOC
    model: recidiviz-staging

  - name: US_MO_SID
    model: recidiviz-staging

  - name: US_MO_FBI
    model: recidiviz-staging

  - name: US_MO_OLN
    model: recidiviz-staging

  elements:
  - name: LBAKRDTA_TAK001
    model: recidiviz-staging

  - name: ORAS_WEEKLY_SUMMARY_UPDATE
    model: recidiviz-staging

  - name: LBAKRDTA_TAK015
    model: recidiviz-staging

  - name: LBAKRDTA_TAK017
    model: recidiviz-staging

  - name: LBAKRDTA_TAK020
    model: recidiviz-staging

  - name: LBAKRDTA_TAK022
    model: recidiviz-staging

  - name: LBAKRDTA_TAK023
    model: recidiviz-staging

  - name: LBAKRDTA_TAK024
    model: recidiviz-staging

  - name: LBAKRDTA_TAK025
    model: recidiviz-staging

  - name: LBAKRDTA_TAK026
    model: recidiviz-staging

  - name: LBAKRDTA_TAK028
    model: recidiviz-staging

  - name: LBAKRDTA_TAK034
    model: recidiviz-staging

  - name: LBAKRDTA_TAK039
    model: recidiviz-staging

  - name: LBAKRDTA_TAK042
    model: recidiviz-staging

  - name: LBAKRDTA_TAK044
    model: recidiviz-staging

  - name: LBAKRDTA_TAK065
    model: recidiviz-staging

  - name: LBAKRDTA_TAK076
    model: recidiviz-staging

  - name: LBAKRDTA_TAK142
    model: recidiviz-staging

  - name: LBAKRDTA_TAK158
    model: recidiviz-staging

  - name: LBAKRDTA_TAK233
    model: recidiviz-staging

  - name: LBAKRDTA_TAK236
    model: recidiviz-staging

  - name: LBAKRDTA_TAK237
    model: recidiviz-staging

  - name: LBAKRDTA_TAK291
    model: recidiviz-staging

  - name: LBAKRDTA_TAK292
    model: recidiviz-staging

  - name: LBAKRDTA_VAK003
    model: recidiviz-staging

  - name: MASTER_PDB_LOCATIONS
    model: recidiviz-staging

  - name: OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW
    model: recidiviz-staging

  - name: OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF
    model: recidiviz-staging

