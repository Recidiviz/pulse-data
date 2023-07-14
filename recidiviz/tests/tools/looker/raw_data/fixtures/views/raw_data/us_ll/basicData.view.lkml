# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "../state_raw_data_shared_fields.view"
view: basicData {
  extends: [
    state_raw_data_shared_fields
  ]
  sql_table_name: {% if view_type._parameter_value == 'raw_data' %} us_ll_raw_data.basicData
    {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.basicData_latest
    {% endif %} ;;

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(${file_id}, ${COL1}) ;;
  }

  dimension: COL1 {
    label: "COL1"
    type: string
    sql: ${TABLE}.COL1 ;;
    group_label: "Primary Key"
  }

  dimension: COL2 {
    label: "COL2"
    type: string
    sql: ${TABLE}.COL2 ;;
  }

  dimension: COL3 {
    label: "COL3"
    type: string
    sql: ${TABLE}.COL3 ;;
  }
}
