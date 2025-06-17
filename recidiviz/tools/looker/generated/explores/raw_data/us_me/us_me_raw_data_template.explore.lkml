# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_me_raw_data_template {
  extension: required

  view_name: us_me_CIS_100_CLIENT
  view_label: "us_me_CIS_100_CLIENT"

  description: "Data pertaining to an individual in Maine"
  group_label: "Raw State Data"
  label: "US_ME Raw Data"
  join: us_me_CIS_102_ALERT_HISTORY {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_102_ALERT_HISTORY.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_102_ALERT_HISTORY"
  }

  join: us_me_CIS_106_ADDRESS {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_106_ADDRESS.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_106_ADDRESS"
  }

  join: us_me_CIS_112_CUSTODY_LEVEL {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_112_CUSTODY_LEVEL.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_112_CUSTODY_LEVEL"
  }

  join: us_me_CIS_116_LSI_HISTORY {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_116_LSI_HISTORY.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_116_LSI_HISTORY"
  }

  join: us_me_CIS_124_SUPERVISION_HISTORY {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_124_SUPERVISION_HISTORY.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_124_SUPERVISION_HISTORY"
  }

  join: us_me_CIS_125_CURRENT_STATUS_HIST {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_125_CURRENT_STATUS_HIST.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_125_CURRENT_STATUS_HIST"
  }

  join: us_me_CIS_128_EMPLOYMENT_HISTORY {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_128_EMPLOYMENT_HISTORY.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_128_EMPLOYMENT_HISTORY"
  }

  join: us_me_CIS_130_INVESTIGATION {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_130_INVESTIGATION.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_130_INVESTIGATION"
  }

  join: us_me_CIS_140_CLASSIFICATION_REVIEW {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_140_CLASSIFICATION_REVIEW.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_140_CLASSIFICATION_REVIEW"
  }

  join: us_me_CIS_160_DRUG_SCREENING {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_160_DRUG_SCREENING.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_160_DRUG_SCREENING"
  }

  join: us_me_CIS_200_CASE_PLAN {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_200_CASE_PLAN.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_200_CASE_PLAN"
  }

  join: us_me_CIS_201_GOALS {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_201_GOALS.Cis_200_Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_201_GOALS"
  }

  join: us_me_CIS_204_GEN_NOTE {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_204_GEN_NOTE.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_204_GEN_NOTE"
  }

  join: us_me_CIS_210_JOB_ASSIGN {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_210_JOB_ASSIGN.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_210_JOB_ASSIGN"
  }

  join: us_me_CIS_215_SEX_OFFENDER_ASSESS {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_215_SEX_OFFENDER_ASSESS.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_215_SEX_OFFENDER_ASSESS"
  }

  join: us_me_CIS_300_Personal_Property {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_300_Personal_Property.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_300_Personal_Property"
  }

  join: us_me_CIS_309_MOVEMENT {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_309_MOVEMENT.Cis_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_309_MOVEMENT"
  }

  join: us_me_CIS_314_TRANSFER {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_314_TRANSFER.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_314_TRANSFER"
  }

  join: us_me_CIS_319_TERM {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_319_TERM.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_319_TERM"
  }

  join: us_me_CIS_324_AWOL {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_324_AWOL.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_324_AWOL"
  }

  join: us_me_CIS_400_CHARGE {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_400_CHARGE.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_400_CHARGE"
  }

  join: us_me_CIS_401_CRT_ORDER_HDR {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_401_CRT_ORDER_HDR.Cis_400_Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_401_CRT_ORDER_HDR"
  }

  join: us_me_CIS_425_MAIN_PROG {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_425_MAIN_PROG.CIS_100_CLIENT_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_425_MAIN_PROG"
  }

  join: us_me_CIS_430_ASSESSMENT {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_430_ASSESSMENT.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_430_ASSESSMENT"
  }

  join: us_me_CIS_462_CLIENTS_INVOLVED {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_462_CLIENTS_INVOLVED.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_462_CLIENTS_INVOLVED"
  }

  join: us_me_CIS_480_VIOLATION {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_480_VIOLATION.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_480_VIOLATION"
  }

  join: us_me_CIS_573_CLIENT_CASE_DETAIL {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_573_CLIENT_CASE_DETAIL.cis_100_client_id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_573_CLIENT_CASE_DETAIL"
  }

  join: us_me_CIS_916_ASSIGN_BED {
    sql_on: ${us_me_CIS_100_CLIENT.Client_Id} = ${us_me_CIS_916_ASSIGN_BED.Cis_100_Client_Id};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_me_CIS_916_ASSIGN_BED"
  }

}
