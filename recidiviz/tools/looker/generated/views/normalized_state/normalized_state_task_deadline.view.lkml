# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_task_deadline {
  sql_table_name: normalized_state.state_task_deadline ;;

  dimension_group: due {
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
    sql: ${TABLE}.due_date ;;
  }

  dimension_group: eligible {
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
    sql: ${TABLE}.eligible_date ;;
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

  dimension: task_deadline_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.task_deadline_id ;;
  }

  dimension: task_metadata {
    type: string
    sql: ${TABLE}.task_metadata ;;
  }

  dimension: task_subtype {
    type: string
    sql: ${TABLE}.task_subtype ;;
  }

  dimension: task_type {
    type: string
    sql: ${TABLE}.task_type ;;
  }

  dimension: task_type_raw_text {
    type: string
    sql: ${TABLE}.task_type_raw_text ;;
  }

  dimension_group: update_datetime {
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
    sql: ${TABLE}.update_datetime ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: count_due_date {
    type: number
    sql: COUNT(${due_date}) ;;
  }

  measure: count_eligible_date {
    type: number
    sql: COUNT(${eligible_date}) ;;
  }

  measure: count_no_date {
    type: sum
    description: "Number of task deadlines with no due date or eligible date"
    sql: CAST(COALESCE(${due_date}, ${eligible_date}) IS NULL AS INT64) ;;
  }
}
