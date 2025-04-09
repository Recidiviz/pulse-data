# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_employment_period {
  sql_table_name: state.state_employment_period ;;

  dimension: employer_address {
    type: string
    sql: ${TABLE}.employer_address ;;
  }

  dimension: employer_name {
    type: string
    sql: ${TABLE}.employer_name ;;
  }

  dimension: employment_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.employment_period_id ;;
  }

  dimension: employment_status {
    type: string
    sql: ${TABLE}.employment_status ;;
  }

  dimension: employment_status_raw_text {
    type: string
    sql: ${TABLE}.employment_status_raw_text ;;
  }

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

  dimension: end_reason {
    type: string
    sql: ${TABLE}.end_reason ;;
  }

  dimension: end_reason_raw_text {
    type: string
    sql: ${TABLE}.end_reason_raw_text ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: job_title {
    type: string
    sql: ${TABLE}.job_title ;;
  }

  dimension_group: last_verified {
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
    sql: ${TABLE}.last_verified_date ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
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
