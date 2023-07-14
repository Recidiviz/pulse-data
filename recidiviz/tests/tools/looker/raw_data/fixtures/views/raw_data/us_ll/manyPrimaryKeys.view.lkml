# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "../state_raw_data_shared_fields.view"
view: manyPrimaryKeys {
  extends: [
    state_raw_data_shared_fields
  ]
  sql_table_name: {% if view_type._parameter_value == 'raw_data' %} us_ll_raw_data.manyPrimaryKeys
    {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.manyPrimaryKeys_latest
    {% endif %} ;;

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(${file_id}, ${col_name_1a}, ${col_name_1b}) ;;
  }

  dimension: col_name_1a {
    label: "col_name_1a"
    type: string
    sql: ${TABLE}.col_name_1a ;;
    description: "First column."
    group_label: "Primary Key"
  }

  dimension: col_name_1b {
    label: "col_name_1b"
    type: string
    sql: ${TABLE}.col_name_1b ;;
    description: "A column description that is long enough to take up
multiple lines. This text block will be interpreted
literally and trailing/leading whitespace is removed."
    group_label: "Primary Key"
  }

  dimension: undocumented_column {
    label: "undocumented_column"
    type: string
    sql: ${TABLE}.undocumented_column ;;
  }
}
