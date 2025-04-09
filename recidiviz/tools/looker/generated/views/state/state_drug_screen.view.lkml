# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_drug_screen {
  sql_table_name: state.state_drug_screen ;;

  dimension_group: drug_screen {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.drug_screen_date ;;
  }

  dimension: drug_screen_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.drug_screen_id ;;
  }

  dimension: drug_screen_metadata {
    type: string
    sql: ${TABLE}.drug_screen_metadata ;;
  }

  dimension: drug_screen_result {
    type: string
    sql: ${TABLE}.drug_screen_result ;;
  }

  dimension: drug_screen_result_raw_text {
    type: string
    sql: ${TABLE}.drug_screen_result_raw_text ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: sample_type {
    type: string
    sql: ${TABLE}.sample_type ;;
  }

  dimension: sample_type_raw_text {
    type: string
    sql: ${TABLE}.sample_type_raw_text ;;
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
