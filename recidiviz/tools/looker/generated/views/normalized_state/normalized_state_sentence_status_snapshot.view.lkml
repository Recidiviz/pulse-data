# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_sentence_status_snapshot {
  sql_table_name: normalized_state.state_sentence_status_snapshot ;;

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: sentence_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.sentence_id ;;
  }

  dimension: sentence_status_snapshot_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.sentence_status_snapshot_id ;;
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

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension_group: status_end_datetime {
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
    sql: ${TABLE}.status_end_datetime ;;
  }

  dimension: status_raw_text {
    type: string
    sql: ${TABLE}.status_raw_text ;;
  }

  dimension_group: status_update_datetime {
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
    sql: ${TABLE}.status_update_datetime ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
