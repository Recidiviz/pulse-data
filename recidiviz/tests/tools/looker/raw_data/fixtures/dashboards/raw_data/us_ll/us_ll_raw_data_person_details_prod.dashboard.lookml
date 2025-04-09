# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ll_raw_data_person_details_prod
  title: Test State Raw Data Person Details Prod
  extends: us_ll_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_OZ_EG
    model: recidiviz-123

  elements:
  - name: basicData
    model: recidiviz-123

  - name: manyPrimaryKeys
    model: recidiviz-123

  - name: datetimeNoParsers
    model: recidiviz-123

  - name: noValidPrimaryKeys
    model: recidiviz-123

  - name: customDatetimeSql
    model: recidiviz-123

