# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_incarceration_sentence {
  sql_table_name: normalized_state.state_incarceration_sentence ;;

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

  dimension: earned_time_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.earned_time_days ;;
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

  dimension: good_time_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.good_time_days ;;
  }

  dimension: incarceration_sentence_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.incarceration_sentence_id ;;
  }

  dimension: incarceration_type {
    type: string
    sql: ${TABLE}.incarceration_type ;;
  }

  dimension: incarceration_type_raw_text {
    type: string
    sql: ${TABLE}.incarceration_type_raw_text ;;
  }

  dimension: initial_time_served_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.initial_time_served_days ;;
  }

  dimension: is_capital_punishment {
    type: yesno
    sql: ${TABLE}.is_capital_punishment ;;
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

  dimension_group: parole_eligibility {
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
    sql: ${TABLE}.parole_eligibility_date ;;
  }

  dimension: parole_possible {
    type: yesno
    sql: ${TABLE}.parole_possible ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension_group: projected_max_release {
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
    sql: ${TABLE}.projected_max_release_date ;;
  }

  dimension_group: projected_min_release {
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
    sql: ${TABLE}.projected_min_release_date ;;
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

  measure: average_earned_time_days {
    type: average
    sql: ${earned_time_days} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: total_earned_time_days {
    type: sum
    sql: ${earned_time_days} ;;
  }
}
