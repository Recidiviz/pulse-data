# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_supervision_case_type_entry {
  sql_table_name: normalized_state.state_supervision_case_type_entry ;;

  dimension: case_type {
    type: string
    sql: ${TABLE}.case_type ;;
  }

  dimension: case_type_raw_text {
    type: string
    sql: ${TABLE}.case_type_raw_text ;;
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

  dimension: supervision_case_type_entry_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_case_type_entry_id ;;
  }

  dimension: supervision_period_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervision_period_id ;;
  }

  measure: count {
    type: count
    drill_fields: [person_id, state_code, case_type, case_type_raw_text, supervision_period_id]
  }
}
