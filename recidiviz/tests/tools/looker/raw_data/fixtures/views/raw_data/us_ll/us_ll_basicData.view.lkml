# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "us_ll_raw_data_shared_fields.view"
view: us_ll_basicData {
  extends: [
    us_ll_raw_data_shared_fields
  ]
  sql_table_name: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} us_ll_raw_data.basicData
    {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.basicData_latest
    {% endif %} ;;

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(IFNULL(CAST(${file_id} AS STRING), ""), IFNULL(CAST(${COL1} AS STRING), "")) ;;
  }

  dimension: COL1 {
    label: "COL1"
    type: string
    description: "Test description"
    sql: ${TABLE}.COL1 ;;
    group_label: "Primary Key"
    full_suggestions: yes
  }

  dimension: COL2 {
    label: "COL2"
    type: string
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.COL2
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  dimension: COL3 {
    label: "COL3"
    type: string
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.COL3
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  measure: count {
    type: count
    drill_fields: [file_id, COL1]
  }

  parameter: view_type {
    type: unquoted
    description: "Used to select whether the view has the most recent version (raw data up to date views) or all data"
    view_label: "Cross Table Filters"
    allowed_value: {
      label: "Raw Data"
      value: "raw_data"
    }
    allowed_value: {
      label: "Raw Data Up To Date Views"
      value: "raw_data_up_to_date_views"
    }
    default_value: "raw_data_up_to_date_views"
  }
}
