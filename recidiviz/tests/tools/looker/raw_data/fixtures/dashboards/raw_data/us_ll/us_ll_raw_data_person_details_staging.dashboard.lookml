# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ll_raw_data_person_details_staging
  title: Test State Raw Data Person Details Staging
  extends: us_ll_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_OZ_EG
    model: recidiviz-staging

  elements:
  - name: basicData
    model: recidiviz-staging

  - name: manyPrimaryKeys
    model: recidiviz-staging

  - name: datetimeNoParsers
    model: recidiviz-staging

  - name: noValidPrimaryKeys
    model: recidiviz-staging

  - name: customDatetimeSql
    model: recidiviz-staging

