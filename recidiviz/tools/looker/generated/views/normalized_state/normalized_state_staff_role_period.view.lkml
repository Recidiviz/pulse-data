# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_staff_role_period {
  sql_table_name: normalized_state.state_staff_role_period ;;

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

  dimension: role_subtype {
    type: string
    sql: ${TABLE}.role_subtype ;;
  }

  dimension: role_subtype_raw_text {
    type: string
    sql: ${TABLE}.role_subtype_raw_text ;;
  }

  dimension: role_type {
    type: string
    sql: ${TABLE}.role_type ;;
  }

  dimension: role_type_raw_text {
    type: string
    sql: ${TABLE}.role_type_raw_text ;;
  }

  dimension: staff_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.staff_id ;;
  }

  dimension: staff_role_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.staff_role_period_id ;;
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
