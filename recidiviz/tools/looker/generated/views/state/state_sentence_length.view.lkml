# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_sentence_length {
  sql_table_name: state.state_sentence_length ;;

  dimension: earned_time_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.earned_time_days ;;
  }

  dimension: good_time_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.good_time_days ;;
  }

  dimension_group: length_update_datetime {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: datetime
    sql: ${TABLE}.length_update_datetime ;;
  }

  dimension_group: parole_eligibility_date_external {
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
    sql: ${TABLE}.parole_eligibility_date_external ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension_group: projected_completion_date_max_external {
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
    sql: ${TABLE}.projected_completion_date_max_external ;;
  }

  dimension_group: projected_completion_date_min_external {
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
    sql: ${TABLE}.projected_completion_date_min_external ;;
  }

  dimension_group: projected_parole_release_date_external {
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
    sql: ${TABLE}.projected_parole_release_date_external ;;
  }

  dimension: sentence_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.sentence_id ;;
  }

  dimension: sentence_length_days_max {
    type: number
    value_format: "0"
    sql: ${TABLE}.sentence_length_days_max ;;
  }

  dimension: sentence_length_days_min {
    type: number
    value_format: "0"
    sql: ${TABLE}.sentence_length_days_min ;;
  }

  dimension: sentence_length_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.sentence_length_id ;;
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

  measure: count {
    type: count
    drill_fields: []
  }
}
