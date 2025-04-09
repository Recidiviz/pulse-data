# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_person_alias {
  sql_table_name: state.state_person_alias ;;

  dimension: alias_type {
    type: string
    sql: ${TABLE}.alias_type ;;
  }

  dimension: alias_type_raw_text {
    type: string
    sql: ${TABLE}.alias_type_raw_text ;;
  }

  dimension: full_name {
    type: string
    sql: ${TABLE}.full_name ;;
  }

  dimension: person_alias_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_alias_id ;;
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
    drill_fields: [person_alias_id, state_code, full_name, alias_type, alias_type_raw_text]
  }
}
