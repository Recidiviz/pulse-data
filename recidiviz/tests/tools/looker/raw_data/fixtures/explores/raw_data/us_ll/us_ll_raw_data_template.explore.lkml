# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_ll_raw_data_template {
  extension: required

  view_name: basicData

  description: "Data pertaining to an individual in Test State"
  group_label: "Raw State Data"
  join: manyPrimaryKeys {
    sql_on: ${basicData.COL1} = ${manyPrimaryKeys.col_name_1a};;
    type: full_outer
    relationship: one_to_one
  }

  join: datetimeNoParsers {
    sql_on: ${basicData.COL1} = ${datetimeNoParsers.COL1};;
    type: full_outer
    relationship: many_to_many
  }

}
