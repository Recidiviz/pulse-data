# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_person_housing_status_period {
  sql_table_name: normalized_state.state_person_housing_status_period ;;

  dimension_group: housing_status_end {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.housing_status_end_date ;;
  }

  dimension_group: housing_status_start {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.housing_status_start_date ;;
  }

  dimension: housing_status_type {
    type: string
    sql: ${TABLE}.housing_status_type ;;
  }

  dimension: housing_status_type_raw_text {
    type: string
    sql: ${TABLE}.housing_status_type_raw_text ;;
  }

  dimension: person_housing_status_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_housing_status_period_id ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
