# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_nd_raw_data_person_details_prod
  title: North Dakota Raw Data Person Details Prod
  extends: us_nd_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_ND_SID
    model: recidiviz-123

  - name: US_ND_ELITE_BOOKING
    model: recidiviz-123

  - name: US_ND_ELITE
    model: recidiviz-123

  elements:
  - name: docstars_offenders
    model: recidiviz-123

  - name: docstars_contacts
    model: recidiviz-123

  - name: docstars_ftr_episode
    model: recidiviz-123

  - name: docstars_lsi_chronology
    model: recidiviz-123

  - name: docstars_offendercasestable
    model: recidiviz-123

  - name: docstars_offensestable
    model: recidiviz-123

  - name: elite_offenderidentifier
    model: recidiviz-123

  - name: elite_offenders
    model: recidiviz-123

  - name: elite_alias
    model: recidiviz-123

  - name: elite_offenderbookingstable
    model: recidiviz-123

  - name: elite_offense_in_custody_and_pos_report_data
    model: recidiviz-123

  - name: recidiviz_elite_offender_alerts
    model: recidiviz-123

  - name: elite_bedassignmenthistory
    model: recidiviz-123

  - name: elite_externalmovements
    model: recidiviz-123

  - name: elite_institutionalactivities
    model: recidiviz-123

  - name: elite_offendersentenceaggs
    model: recidiviz-123

  - name: elite_offendersentences
    model: recidiviz-123

  - name: elite_offendersentenceterms
    model: recidiviz-123

  - name: elite_orderstable
    model: recidiviz-123

