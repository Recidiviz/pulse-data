# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_charge_supervision_sentence_association {
  sql_table_name: state.state_charge_supervision_sentence_association ;;

  dimension: charge_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.charge_id ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    sql: CONCAT(${TABLE}.charge_id, "_", ${TABLE}.supervision_sentence_id) ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: supervision_sentence_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervision_sentence_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
