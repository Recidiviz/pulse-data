# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_dashboard_generator.py`.

- dashboard: us_ll_raw_data_person_details
  title: Test State Raw Data Person Details
  description: For examining individuals in US_LL's raw data tables
  layout: newspaper
  load_configuration: wait

  filters:
  - name: View Type
    title: View Type
    type: field_filter
    default_value: raw^_data^_up^_to^_date^_views
    allow_multiple_values: false
    required: true
    ui_config: 
      type: dropdown_menu
      display: inline
    model: "@{project_id}"
    explore: us_ll_raw_data
    field: us_ll_basicData.view_type

  - name: US_OZ_EG
    title: US_OZ_EG
    type: field_filter
    default_value: ""
    allow_multiple_values: true
    required: false
    ui_config: 
      type: tag_list
      display: popover
    model: "@{project_id}"
    explore: us_ll_raw_data
    field: us_ll_basicData.COL1

  elements:
  - name: basicData
    title: basicData
    explore: us_ll_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ll_basicData.primary_key,
      us_ll_basicData.COL1,
      us_ll_basicData.COL2,
      us_ll_basicData.COL3,
      us_ll_basicData.file_id,
      us_ll_basicData.is_deleted]
    sorts: [us_ll_basicData.COL1]
    note_display: hover
    note_text: "tagBasicData file description"
    listen: 
      View Type: us_ll_basicData.view_type
      US_OZ_EG: us_ll_basicData.COL1
    row: 0
    col: 0
    width: 24
    height: 6

  - name: manyPrimaryKeys
    title: manyPrimaryKeys
    explore: us_ll_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ll_manyPrimaryKeys.primary_key,
      us_ll_manyPrimaryKeys.col_name_1a,
      us_ll_manyPrimaryKeys.col_name_1b,
      us_ll_manyPrimaryKeys.undocumented_column,
      us_ll_manyPrimaryKeys.file_id,
      us_ll_manyPrimaryKeys.is_deleted]
    sorts: [us_ll_manyPrimaryKeys.col_name_1a, us_ll_manyPrimaryKeys.col_name_1b]
    note_display: hover
    note_text: "First raw file."
    listen: 
      View Type: us_ll_basicData.view_type
      US_OZ_EG: us_ll_basicData.COL1
    row: 6
    col: 0
    width: 24
    height: 6

  - name: datetimeNoParsers
    title: datetimeNoParsers
    explore: us_ll_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ll_datetimeNoParsers.primary_key,
      us_ll_datetimeNoParsers.COL1,
      us_ll_datetimeNoParsers.COL2__raw,
      us_ll_datetimeNoParsers.file_id,
      us_ll_datetimeNoParsers.is_deleted]
    sorts: [us_ll_datetimeNoParsers.COL2__raw]
    note_display: hover
    note_text: "Testing datetime field with no datetime sql parsers defined"
    listen: 
      View Type: us_ll_basicData.view_type
      US_OZ_EG: us_ll_basicData.COL1
    row: 12
    col: 0
    width: 24
    height: 6

  - name: noValidPrimaryKeys
    title: noValidPrimaryKeys
    explore: us_ll_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ll_noValidPrimaryKeys.primary_key,
      us_ll_noValidPrimaryKeys.col_name_1a,
      us_ll_noValidPrimaryKeys.col_name_1b,
      us_ll_noValidPrimaryKeys.undocumented_column,
      us_ll_noValidPrimaryKeys.file_id,
      us_ll_noValidPrimaryKeys.is_deleted]
    sorts: []
    note_display: hover
    note_text: "First raw file."
    listen: 
      View Type: us_ll_basicData.view_type
      US_OZ_EG: us_ll_basicData.COL1
    row: 18
    col: 0
    width: 24
    height: 6

  - name: customDatetimeSql
    title: customDatetimeSql
    explore: us_ll_raw_data
    model: "@{project_id}"
    type: looker_grid
    fields: [us_ll_customDatetimeSql.primary_key,
      us_ll_customDatetimeSql.COL1,
      us_ll_customDatetimeSql.COL2__raw,
      us_ll_customDatetimeSql.file_id,
      us_ll_customDatetimeSql.is_deleted]
    sorts: [us_ll_customDatetimeSql.COL2__raw]
    note_display: hover
    note_text: "Testing custom datetime sql parsing associated with the datetime field"
    listen: 
      View Type: us_ll_basicData.view_type
      US_OZ_EG: us_ll_basicData.COL1
    row: 24
    col: 0
    width: 24
    height: 6

