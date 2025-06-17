# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_az_raw_data_person_details_prod
  title: Arizona Raw Data Person Details Prod
  extends: us_az_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_AZ_PERSON_ID
    model: recidiviz-123

  elements:
  - name: PERSON
    model: recidiviz-123

  - name: DEMOGRAPHICS
    model: recidiviz-123

  - name: OCCUPANCY
    model: recidiviz-123

  - name: DPP_EPISODE
    model: recidiviz-123

  - name: DOC_EPISODE
    model: recidiviz-123

  - name: LOOKUPS
    model: recidiviz-123

