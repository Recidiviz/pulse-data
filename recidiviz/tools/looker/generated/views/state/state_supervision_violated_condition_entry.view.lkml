# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_supervision_violated_condition_entry {
  sql_table_name: state.state_supervision_violated_condition_entry ;;

  dimension: condition {
    type: string
    sql: ${TABLE}.condition ;;
  }

  dimension: condition_raw_text {
    type: string
    sql: ${TABLE}.condition_raw_text ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: supervision_violated_condition_entry_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_violated_condition_entry_id ;;
  }

  dimension: supervision_violation_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervision_violation_id ;;
  }

  measure: count {
    type: count
    drill_fields: [person_id, state_code, condition]
  }
}
