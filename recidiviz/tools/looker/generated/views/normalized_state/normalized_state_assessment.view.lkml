# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_assessment {
  sql_table_name: normalized_state.state_assessment ;;

  dimension_group: assessment {
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
    sql: ${TABLE}.assessment_date ;;
  }

  dimension: assessment_class {
    type: string
    sql: ${TABLE}.assessment_class ;;
  }

  dimension: assessment_class_raw_text {
    type: string
    sql: ${TABLE}.assessment_class_raw_text ;;
  }

  dimension: assessment_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.assessment_id ;;
  }

  dimension: assessment_level {
    type: string
    sql: ${TABLE}.assessment_level ;;
  }

  dimension: assessment_level_raw_text {
    type: string
    sql: ${TABLE}.assessment_level_raw_text ;;
  }

  dimension: assessment_metadata {
    type: string
    sql: ${TABLE}.assessment_metadata ;;
  }

  dimension: assessment_score {
    type: number
    value_format: "0"
    sql: ${TABLE}.assessment_score ;;
  }

  dimension: assessment_score_bucket {
    type: string
    sql: ${TABLE}.assessment_score_bucket ;;
  }

  dimension: assessment_type {
    type: string
    sql: ${TABLE}.assessment_type ;;
  }

  dimension: assessment_type_raw_text {
    type: string
    sql: ${TABLE}.assessment_type_raw_text ;;
  }

  dimension: conducting_staff_external_id {
    type: string
    sql: ${TABLE}.conducting_staff_external_id ;;
  }

  dimension: conducting_staff_external_id_type {
    type: string
    sql: ${TABLE}.conducting_staff_external_id_type ;;
  }

  dimension: conducting_staff_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.conducting_staff_id ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: sequence_num {
    type: number
    value_format: "0"
    sql: ${TABLE}.sequence_num ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  measure: average_assessment_score {
    type: average
    sql: ${assessment_score} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: total_assessment_score {
    type: sum
    sql: ${assessment_score} ;;
  }
}
