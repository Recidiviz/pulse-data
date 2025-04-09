# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_charge_v2_state_sentence_association {
  sql_table_name: state.state_charge_v2_state_sentence_association ;;

  dimension: charge_v2_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.charge_v2_id ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    sql: CONCAT(${TABLE}.charge_v2_id, "_", ${TABLE}.sentence_id) ;;
  }

  dimension: sentence_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.sentence_id ;;
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
