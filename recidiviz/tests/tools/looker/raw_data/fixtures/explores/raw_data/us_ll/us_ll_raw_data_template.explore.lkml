# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_ll_raw_data_template {
  extension: required

  view_name: us_ll_basicData
  view_label: "us_ll_basicData"

  description: "Data pertaining to an individual in Test State"
  group_label: "Raw State Data"
  label: "US_LL Raw Data"
  join: us_ll_manyPrimaryKeys {
    sql_on: ${us_ll_basicData.COL1} = ${us_ll_manyPrimaryKeys.col_name_1a};;
    type: full_outer
    relationship: one_to_one
    view_label: "us_ll_manyPrimaryKeys"
  }

  join: us_ll_datetimeNoParsers {
    sql_on: REPLACE(${us_ll_basicData.COL1}, "a", "b") = REPLACE(${us_ll_datetimeNoParsers.COL1}, "x", "y");;
    type: full_outer
    relationship: many_to_many
    view_label: "us_ll_datetimeNoParsers"
  }

  join: us_ll_noValidPrimaryKeys {
    sql_on: ${us_ll_basicData.COL1} = ${us_ll_noValidPrimaryKeys.col_name_1a};;
    type: full_outer
    relationship: many_to_one
    view_label: "us_ll_noValidPrimaryKeys"
  }

  join: us_ll_customDatetimeSql {
    sql_on: ${us_ll_basicData.COL1} = ${us_ll_customDatetimeSql.COL1};;
    type: full_outer
    relationship: one_to_many
    view_label: "us_ll_customDatetimeSql"
  }

}
