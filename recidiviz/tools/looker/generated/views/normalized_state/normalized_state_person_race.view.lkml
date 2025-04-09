# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_person_race {
  sql_table_name: normalized_state.state_person_race ;;

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: person_race_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_race_id ;;
  }

  dimension: race {
    type: string
    sql: ${TABLE}.race ;;
  }

  dimension: race_raw_text {
    type: string
    sql: ${TABLE}.race_raw_text ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  measure: count {
    type: count
    drill_fields: [person_id, state_code, race, race_raw_text]
  }
}
