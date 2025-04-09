# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_person_ethnicity {
  sql_table_name: state.state_person_ethnicity ;;

  dimension: ethnicity {
    type: string
    sql: ${TABLE}.ethnicity ;;
  }

  dimension: ethnicity_raw_text {
    type: string
    sql: ${TABLE}.ethnicity_raw_text ;;
  }

  dimension: person_ethnicity_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_ethnicity_id ;;
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
    drill_fields: [person_ethnicity_id, state_code, ethnicity, ethnicity_raw_text]
  }
}
