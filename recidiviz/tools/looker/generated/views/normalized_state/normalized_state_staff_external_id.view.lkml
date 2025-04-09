# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_staff_external_id {
  sql_table_name: normalized_state.state_staff_external_id ;;

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: id_type {
    type: string
    sql: ${TABLE}.id_type ;;
  }

  dimension: staff_external_id_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.staff_external_id_id ;;
  }

  dimension: staff_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.staff_id ;;
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
