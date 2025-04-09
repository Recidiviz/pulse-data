# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_staff_location_period {
  sql_table_name: normalized_state.state_staff_location_period ;;

  dimension_group: end {
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
    sql: ${TABLE}.end_date ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: location_external_id {
    type: string
    sql: ${TABLE}.location_external_id ;;
  }

  dimension: staff_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.staff_id ;;
  }

  dimension: staff_location_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.staff_location_period_id ;;
  }

  dimension_group: start {
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
    sql: ${TABLE}.start_date ;;
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
