# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_or_raw_data_person_details_staging
  title: Oregon Raw Data Person Details Staging
  extends: us_or_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_OR_RECORD_KEY
    model: recidiviz-staging

  elements:
  - name: RCDVZ_PRDDTA_OP970P
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CLOVER
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CMCROH
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CMOFFT
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CMOFRH
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CMSACN
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CMSACO
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_CMSAIM
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_MTOFDR
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_MTRULE
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_MTSANC
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_OPCOND
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDTA_OPCONE
    model: recidiviz-staging

  - name: RCDVZ_CISPRDDT_CLCLHD
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP007P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP008P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP009P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP010P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP011P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP013P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP053P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OP054P
    model: recidiviz-staging

  - name: RCDVZ_PRDDTA_OPCOUR
    model: recidiviz-staging

