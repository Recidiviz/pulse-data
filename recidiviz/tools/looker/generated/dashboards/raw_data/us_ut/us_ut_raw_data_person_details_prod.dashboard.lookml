# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ut_raw_data_person_details_prod
  title: Utah Raw Data Person Details Prod
  extends: us_ut_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_UT_DOC
    model: recidiviz-123

  elements:
  - name: ofndr
    model: recidiviz-123

  - name: cap
    model: recidiviz-123

  - name: ofndr_addr_arch
    model: recidiviz-123

  - name: ofndr_agnt
    model: recidiviz-123

  - name: ofndr_cap_actvty
    model: recidiviz-123

  - name: ofndr_dio_prog
    model: recidiviz-123

  - name: ofndr_dob
    model: recidiviz-123

  - name: ofndr_email
    model: recidiviz-123

  - name: ofndr_lgl_stat
    model: recidiviz-123

  - name: ofndr_loc_hist
    model: recidiviz-123

  - name: ofndr_name
    model: recidiviz-123

  - name: ofndr_norm
    model: recidiviz-123

  - name: ofndr_oth_num
    model: recidiviz-123

  - name: ofndr_phone
    model: recidiviz-123

  - name: ofndr_prgrmng
    model: recidiviz-123

  - name: ofndr_sec_assess
    model: recidiviz-123

  - name: ofndr_spcl_sprvsn
    model: recidiviz-123

  - name: ofndr_sprvsn
    model: recidiviz-123

  - name: ofndr_tst
    model: recidiviz-123

