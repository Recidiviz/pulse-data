# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_pa_raw_data_person_details_staging
  title: Pennsylvania Raw Data Person Details Staging
  extends: us_pa_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-staging

  - name: US_PA_INMATE
    model: recidiviz-staging

  - name: US_PA_CONT
    model: recidiviz-staging

  - name: US_PA_PBPP
    model: recidiviz-staging

  elements:
  - name: dbo_tblSearchInmateInfo
    model: recidiviz-staging

  - name: RECIDIVIZ_REFERENCE_control_number_linking_ids
    model: recidiviz-staging

  - name: dbo_BoardAction
    model: recidiviz-staging

  - name: dbo_ConditionCode
    model: recidiviz-staging

  - name: dbo_ConditionCodeDescription
    model: recidiviz-staging

  - name: dbo_DOB
    model: recidiviz-staging

  - name: dbo_DeActivateParoleNumber
    model: recidiviz-staging

  - name: dbo_Hist_Parolee
    model: recidiviz-staging

  - name: dbo_Hist_Release
    model: recidiviz-staging

  - name: dbo_Hist_SanctionTracking
    model: recidiviz-staging

  - name: dbo_Hist_Treatment
    model: recidiviz-staging

  - name: dbo_LSIHistory
    model: recidiviz-staging

  - name: dbo_LSIR
    model: recidiviz-staging

  - name: dbo_Miscon
    model: recidiviz-staging

  - name: dbo_Movrec
    model: recidiviz-staging

  - name: dbo_Offender
    model: recidiviz-staging

  - name: dbo_ParoleCount
    model: recidiviz-staging

  - name: dbo_Parolee
    model: recidiviz-staging

  - name: dbo_Perrec
    model: recidiviz-staging

  - name: dbo_RelAgentHistory
    model: recidiviz-staging

  - name: dbo_RelEmployment
    model: recidiviz-staging

  - name: dbo_Release
    model: recidiviz-staging

  - name: dbo_ReleaseInfo
    model: recidiviz-staging

  - name: dbo_SanctionTracking
    model: recidiviz-staging

  - name: dbo_Senrec
    model: recidiviz-staging

  - name: dbo_Sentence
    model: recidiviz-staging

  - name: dbo_SentenceGroup
    model: recidiviz-staging

  - name: dbo_Treatment
    model: recidiviz-staging

  - name: dbo_tblInmTestScore
    model: recidiviz-staging

  - name: dbo_tblInmTestScoreHist
    model: recidiviz-staging

  - name: dbo_vwCCISAllMvmt
    model: recidiviz-staging

