# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_nd_raw_data_person_details_staging
  title: North Dakota Raw Data Person Details Staging
  extends: us_nd_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_ND_SID
    model: recidiviz-staging

  - name: US_ND_ELITE_BOOKING
    model: recidiviz-staging

  - name: US_ND_ELITE
    model: recidiviz-staging

  elements:
  - name: docstars_offenders
    model: recidiviz-staging

  - name: docstars_contacts
    model: recidiviz-staging

  - name: docstars_ftr_episode
    model: recidiviz-staging

  - name: docstars_lsi_chronology
    model: recidiviz-staging

  - name: docstars_offendercasestable
    model: recidiviz-staging

  - name: docstars_offensestable
    model: recidiviz-staging

  - name: elite_offenderidentifier
    model: recidiviz-staging

  - name: elite_offenders
    model: recidiviz-staging

  - name: elite_alias
    model: recidiviz-staging

  - name: elite_offenderbookingstable
    model: recidiviz-staging

  - name: elite_offense_in_custody_and_pos_report_data
    model: recidiviz-staging

  - name: recidiviz_elite_offender_alerts
    model: recidiviz-staging

  - name: elite_bedassignmenthistory
    model: recidiviz-staging

  - name: elite_externalmovements
    model: recidiviz-staging

  - name: elite_institutionalactivities
    model: recidiviz-staging

  - name: elite_offendersentenceaggs
    model: recidiviz-staging

  - name: elite_offendersentences
    model: recidiviz-staging

  - name: elite_offendersentenceterms
    model: recidiviz-staging

  - name: elite_orderstable
    model: recidiviz-staging

