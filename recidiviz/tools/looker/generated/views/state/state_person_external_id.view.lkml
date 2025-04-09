# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_person_external_id {
  sql_table_name: state.state_person_external_id ;;

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: external_id_with_type {
    type: string
    sql: CONCAT(${external_id}, " (", ${id_type}, ")") ;;
  }

  dimension: id_type {
    type: string
    sql: ${TABLE}.id_type ;;
  }

  dimension: person_external_id_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_external_id_id ;;
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

  measure: count {
    type: count
    drill_fields: [state_code, external_id, id_type]
  }

  measure: list_external_id_with_type {
    type: list
    list_field: external_id_with_type
  }
}
