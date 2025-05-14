# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_incarceration_incident_outcome {
  sql_table_name: normalized_state.state_incarceration_incident_outcome ;;

  dimension_group: date_effective {
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
    sql: ${TABLE}.date_effective ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension_group: hearing {
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
    sql: ${TABLE}.hearing_date ;;
  }

  dimension: incarceration_incident_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.incarceration_incident_id ;;
  }

  dimension: incarceration_incident_outcome_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.incarceration_incident_outcome_id ;;
  }

  dimension: outcome_description {
    type: string
    sql: ${TABLE}.outcome_description ;;
  }

  dimension: outcome_metadata {
    type: string
    sql: ${TABLE}.outcome_metadata ;;
  }

  dimension: outcome_type {
    type: string
    sql: ${TABLE}.outcome_type ;;
  }

  dimension: outcome_type_raw_text {
    type: string
    sql: ${TABLE}.outcome_type_raw_text ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension_group: projected_end {
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
    sql: ${TABLE}.projected_end_date ;;
  }

  dimension: punishment_length_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.punishment_length_days ;;
  }

  dimension_group: report {
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
    sql: ${TABLE}.report_date ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  measure: average_punishment_length_days {
    type: average
    sql: ${punishment_length_days} ;;
  }

  measure: count {
    type: count
    drill_fields: [person_id, state_code, hearing_date, date_effective_date, punishment_length_days, outcome_type, outcome_type_raw_text, outcome_description, incarceration_incident_outcome_id]
  }

  measure: total_punishment_length_days {
    type: sum
    sql: ${punishment_length_days} ;;
  }
}
