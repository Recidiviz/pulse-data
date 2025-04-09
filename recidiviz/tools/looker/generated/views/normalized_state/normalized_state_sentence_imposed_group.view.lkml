# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_sentence_imposed_group {
  sql_table_name: normalized_state.state_sentence_imposed_group ;;

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

  dimension: most_severe_charge_v2_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.most_severe_charge_v2_id ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: sentence_imposed_group_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.sentence_imposed_group_id ;;
  }

  dimension: sentencing_authority {
    type: string
    sql: ${TABLE}.sentencing_authority ;;
  }

  dimension_group: serving_start {
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
    sql: ${TABLE}.serving_start_date ;;
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
