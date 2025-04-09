# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "us_ll_raw_data_shared_fields.view"
view: us_ll_manyPrimaryKeys {
  extends: [
    us_ll_raw_data_shared_fields
  ]
  sql_table_name: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} us_ll_raw_data.manyPrimaryKeys
    {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.manyPrimaryKeys_latest
    {% endif %} ;;

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(IFNULL(CAST(${file_id} AS STRING), ""), IFNULL(CAST(${col_name_1a} AS STRING), ""), IFNULL(CAST(${col_name_1b} AS STRING), "")) ;;
  }

  dimension: col_name_1a {
    label: "col_name_1a"
    type: string
    description: "First column."
    sql: ${TABLE}.col_name_1a ;;
    group_label: "Primary Key"
  }

  dimension: col_name_1b {
    label: "col_name_1b"
    type: string
    description: "A column description that is long enough to take up
multiple lines. This text block will be interpreted
literally and trailing/leading whitespace is removed."
    sql: ${TABLE}.col_name_1b ;;
    group_label: "Primary Key"
  }

  dimension: undocumented_column {
    label: "undocumented_column"
    type: string
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.undocumented_column
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  measure: count {
    type: count
    drill_fields: [file_id, col_name_1a, col_name_1b]
  }
}
