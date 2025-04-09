# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_supervision_violation_response {
  sql_table_name: state.state_supervision_violation_response ;;

  dimension: deciding_body_type {
    type: string
    sql: ${TABLE}.deciding_body_type ;;
  }

  dimension: deciding_body_type_raw_text {
    type: string
    sql: ${TABLE}.deciding_body_type_raw_text ;;
  }

  dimension: deciding_staff_external_id {
    type: string
    sql: ${TABLE}.deciding_staff_external_id ;;
  }

  dimension: deciding_staff_external_id_type {
    type: string
    sql: ${TABLE}.deciding_staff_external_id_type ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: is_draft {
    type: yesno
    sql: ${TABLE}.is_draft ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension_group: response {
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
    sql: ${TABLE}.response_date ;;
  }

  dimension: response_subtype {
    type: string
    sql: ${TABLE}.response_subtype ;;
  }

  dimension: response_type {
    type: string
    sql: ${TABLE}.response_type ;;
  }

  dimension: response_type_raw_text {
    type: string
    sql: ${TABLE}.response_type_raw_text ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: supervision_violation_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervision_violation_id ;;
  }

  dimension: supervision_violation_response_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_violation_response_id ;;
  }

  dimension: violation_response_metadata {
    type: string
    sql: ${TABLE}.violation_response_metadata ;;
  }

  dimension: violation_response_severity {
    type: string
    sql: ${TABLE}.violation_response_severity ;;
  }

  dimension: violation_response_severity_raw_text {
    type: string
    sql: ${TABLE}.violation_response_severity_raw_text ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
