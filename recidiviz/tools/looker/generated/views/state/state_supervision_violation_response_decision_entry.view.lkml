# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_supervision_violation_response_decision_entry {
  sql_table_name: state.state_supervision_violation_response_decision_entry ;;

  dimension: decision {
    type: string
    sql: ${TABLE}.decision ;;
  }

  dimension: decision_raw_text {
    type: string
    sql: ${TABLE}.decision_raw_text ;;
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

  dimension: supervision_violation_response_decision_entry_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_violation_response_decision_entry_id ;;
  }

  dimension: supervision_violation_response_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervision_violation_response_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
