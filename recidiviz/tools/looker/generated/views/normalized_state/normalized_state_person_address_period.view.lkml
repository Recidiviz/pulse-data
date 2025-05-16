# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_person_address_period {
  sql_table_name: normalized_state.state_person_address_period ;;

  dimension: address_city {
    type: string
    sql: ${TABLE}.address_city ;;
  }

  dimension: address_country {
    type: string
    sql: ${TABLE}.address_country ;;
  }

  dimension: address_county {
    type: string
    sql: ${TABLE}.address_county ;;
  }

  dimension_group: address_end {
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
    sql: ${TABLE}.address_end_date ;;
  }

  dimension: address_is_verified {
    type: yesno
    sql: ${TABLE}.address_is_verified ;;
  }

  dimension: address_line_1 {
    type: string
    sql: ${TABLE}.address_line_1 ;;
  }

  dimension: address_line_2 {
    type: string
    sql: ${TABLE}.address_line_2 ;;
  }

  dimension: address_metadata {
    type: string
    sql: ${TABLE}.address_metadata ;;
  }

  dimension_group: address_start {
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
    sql: ${TABLE}.address_start_date ;;
  }

  dimension: address_state {
    type: string
    sql: ${TABLE}.address_state ;;
  }

  dimension: address_type {
    type: string
    sql: ${TABLE}.address_type ;;
  }

  dimension: address_type_raw_text {
    type: string
    sql: ${TABLE}.address_type_raw_text ;;
  }

  dimension: address_zip {
    type: string
    sql: ${TABLE}.address_zip ;;
  }

  dimension: full_address {
    type: string
    sql: ${TABLE}.full_address ;;
  }

  dimension: person_address_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_address_period_id ;;
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
