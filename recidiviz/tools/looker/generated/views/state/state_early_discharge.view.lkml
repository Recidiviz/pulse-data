# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_early_discharge {
  sql_table_name: state.state_early_discharge ;;

  dimension: county_code {
    type: string
    sql: ${TABLE}.county_code ;;
  }

  dimension: deciding_body_type {
    type: string
    sql: ${TABLE}.deciding_body_type ;;
  }

  dimension: deciding_body_type_raw_text {
    type: string
    sql: ${TABLE}.deciding_body_type_raw_text ;;
  }

  dimension_group: decision {
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
    sql: ${TABLE}.decision_date ;;
  }

  dimension: decision {
    type: string
    sql: ${TABLE}.decision ;;
  }

  dimension: decision_raw_text {
    type: string
    sql: ${TABLE}.decision_raw_text ;;
  }

  dimension: decision_status {
    type: string
    sql: ${TABLE}.decision_status ;;
  }

  dimension: decision_status_raw_text {
    type: string
    sql: ${TABLE}.decision_status_raw_text ;;
  }

  dimension: early_discharge_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.early_discharge_id ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: incarceration_sentence_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.incarceration_sentence_id ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension_group: request {
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
    sql: ${TABLE}.request_date ;;
  }

  dimension: requesting_body_type {
    type: string
    sql: ${TABLE}.requesting_body_type ;;
  }

  dimension: requesting_body_type_raw_text {
    type: string
    sql: ${TABLE}.requesting_body_type_raw_text ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: supervision_sentence_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervision_sentence_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
