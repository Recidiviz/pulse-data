# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ut_raw_data_person_details_staging
  title: Utah Raw Data Person Details Staging
  extends: us_ut_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_UT_DOC
    model: recidiviz-staging

  elements:
  - name: ofndr
    model: recidiviz-staging

  - name: cap
    model: recidiviz-staging

  - name: ofndr_addr_arch
    model: recidiviz-staging

  - name: ofndr_agnt
    model: recidiviz-staging

  - name: ofndr_cap_actvty
    model: recidiviz-staging

  - name: ofndr_dio_prog
    model: recidiviz-staging

  - name: ofndr_dob
    model: recidiviz-staging

  - name: ofndr_email
    model: recidiviz-staging

  - name: ofndr_lgl_stat
    model: recidiviz-staging

  - name: ofndr_loc_hist
    model: recidiviz-staging

  - name: ofndr_name
    model: recidiviz-staging

  - name: ofndr_norm
    model: recidiviz-staging

  - name: ofndr_oth_num
    model: recidiviz-staging

  - name: ofndr_phone
    model: recidiviz-staging

  - name: ofndr_prgrmng
    model: recidiviz-staging

  - name: ofndr_sec_assess
    model: recidiviz-staging

  - name: ofndr_spcl_sprvsn
    model: recidiviz-staging

  - name: ofndr_sprvsn
    model: recidiviz-staging

  - name: ofndr_tst
    model: recidiviz-staging

