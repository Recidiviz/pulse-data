# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_sentence {
  sql_table_name: state.state_sentence ;;

  dimension: conditions {
    type: string
    sql: ${TABLE}.conditions ;;
  }

  dimension: county_code {
    type: string
    sql: ${TABLE}.county_code ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension_group: imposed {
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
    sql: ${TABLE}.imposed_date ;;
  }

  dimension: initial_time_served_days {
    type: number
    value_format: "0"
    sql: ${TABLE}.initial_time_served_days ;;
  }

  dimension: is_capital_punishment {
    type: yesno
    sql: ${TABLE}.is_capital_punishment ;;
  }

  dimension: is_life {
    type: yesno
    sql: ${TABLE}.is_life ;;
  }

  dimension: parent_sentence_external_id_array {
    type: string
    sql: ${TABLE}.parent_sentence_external_id_array ;;
  }

  dimension: parole_possible {
    type: yesno
    sql: ${TABLE}.parole_possible ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: sentence_group_external_id {
    type: string
    sql: ${TABLE}.sentence_group_external_id ;;
  }

  dimension: sentence_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.sentence_id ;;
  }

  dimension: sentence_metadata {
    type: string
    sql: ${TABLE}.sentence_metadata ;;
  }

  dimension: sentence_type {
    type: string
    sql: ${TABLE}.sentence_type ;;
  }

  dimension: sentence_type_raw_text {
    type: string
    sql: ${TABLE}.sentence_type_raw_text ;;
  }

  dimension: sentencing_authority {
    type: string
    sql: ${TABLE}.sentencing_authority ;;
  }

  dimension: sentencing_authority_raw_text {
    type: string
    sql: ${TABLE}.sentencing_authority_raw_text ;;
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
