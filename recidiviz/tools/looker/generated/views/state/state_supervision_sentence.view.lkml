# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_supervision_sentence {
  sql_table_name: state.state_supervision_sentence ;;

  dimension_group: completion {
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
    sql: ${TABLE}.completion_date ;;
  }

  dimension: conditions {
    type: string
    sql: ${TABLE}.conditions ;;
  }

  dimension: county_code {
    type: string
    sql: ${TABLE}.county_code ;;
  }

  dimension_group: date_imposed {
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
    sql: ${TABLE}.date_imposed ;;
  }

  dimension_group: effective {
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
    sql: ${TABLE}.effective_date ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: is_life {
    type: yesno
    sql: ${TABLE}.is_life ;;
  }

  dimension: max_length_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.max_length_days ;;
  }

  dimension: min_length_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.min_length_days ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension_group: projected_completion {
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
    sql: ${TABLE}.projected_completion_date ;;
  }

  dimension: sentence_metadata {
    type: string
    sql: ${TABLE}.sentence_metadata ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: status_raw_text {
    type: string
    sql: ${TABLE}.status_raw_text ;;
  }

  dimension: supervision_sentence_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_sentence_id ;;
  }

  dimension: supervision_type {
    type: string
    sql: ${TABLE}.supervision_type ;;
  }

  dimension: supervision_type_raw_text {
    type: string
    sql: ${TABLE}.supervision_type_raw_text ;;
  }

  measure: average_max_length_days {
    type: average
    sql: ${max_length_days} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: total_max_length_days {
    type: sum
    sql: ${max_length_days} ;;
  }
}
