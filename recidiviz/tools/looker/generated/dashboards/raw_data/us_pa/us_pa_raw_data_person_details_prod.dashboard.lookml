# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_pa_raw_data_person_details_prod
  title: Pennsylvania Raw Data Person Details Prod
  extends: us_pa_raw_data_person_details_template

  filters:
  - name: View Type
    model: recidiviz-123

  - name: US_PA_INMATE
    model: recidiviz-123

  - name: US_PA_CONT
    model: recidiviz-123

  - name: US_PA_PBPP
    model: recidiviz-123

  elements:
  - name: dbo_tblSearchInmateInfo
    model: recidiviz-123

  - name: RECIDIVIZ_REFERENCE_control_number_linking_ids
    model: recidiviz-123

  - name: dbo_BoardAction
    model: recidiviz-123

  - name: dbo_ConditionCode
    model: recidiviz-123

  - name: dbo_ConditionCodeDescription
    model: recidiviz-123

  - name: dbo_DOB
    model: recidiviz-123

  - name: dbo_DeActivateParoleNumber
    model: recidiviz-123

  - name: dbo_Hist_Parolee
    model: recidiviz-123

  - name: dbo_Hist_Release
    model: recidiviz-123

  - name: dbo_Hist_SanctionTracking
    model: recidiviz-123

  - name: dbo_Hist_Treatment
    model: recidiviz-123

  - name: dbo_LSIHistory
    model: recidiviz-123

  - name: dbo_LSIR
    model: recidiviz-123

  - name: dbo_Miscon
    model: recidiviz-123

  - name: dbo_Movrec
    model: recidiviz-123

  - name: dbo_Offender
    model: recidiviz-123

  - name: dbo_ParoleCount
    model: recidiviz-123

  - name: dbo_Parolee
    model: recidiviz-123

  - name: dbo_Perrec
    model: recidiviz-123

  - name: dbo_RelAgentHistory
    model: recidiviz-123

  - name: dbo_RelEmployment
    model: recidiviz-123

  - name: dbo_Release
    model: recidiviz-123

  - name: dbo_ReleaseInfo
    model: recidiviz-123

  - name: dbo_SanctionTracking
    model: recidiviz-123

  - name: dbo_Senrec
    model: recidiviz-123

  - name: dbo_Sentence
    model: recidiviz-123

  - name: dbo_SentenceGroup
    model: recidiviz-123

  - name: dbo_Treatment
    model: recidiviz-123

  - name: dbo_tblInmTestScore
    model: recidiviz-123

  - name: dbo_tblInmTestScoreHist
    model: recidiviz-123

  - name: dbo_vwCCISAllMvmt
    model: recidiviz-123

