# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_az_raw_data_person_details_staging
  title: Arizona Raw Data Person Details Staging
  extends: us_az_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_AZ_PERSON_ID
    model: recidiviz-staging

  elements:
  - name: PERSON
    model: recidiviz-staging

  - name: DEMOGRAPHICS
    model: recidiviz-staging

  - name: OCCUPANCY
    model: recidiviz-staging

  - name: DPP_EPISODE
    model: recidiviz-staging

  - name: DOC_EPISODE
    model: recidiviz-staging

  - name: LOOKUPS
    model: recidiviz-staging

