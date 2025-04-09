# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_person_staff_relationship_period {
  sql_table_name: normalized_state.state_person_staff_relationship_period ;;

  dimension: associated_staff_external_id {
    type: string
    sql: ${TABLE}.associated_staff_external_id ;;
  }

  dimension: associated_staff_external_id_type {
    type: string
    sql: ${TABLE}.associated_staff_external_id_type ;;
  }

  dimension: associated_staff_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.associated_staff_id ;;
  }

  dimension: location_external_id {
    type: string
    sql: ${TABLE}.location_external_id ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: person_staff_relationship_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_staff_relationship_period_id ;;
  }

  dimension_group: relationship_end_date_exclusive {
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
    sql: ${TABLE}.relationship_end_date_exclusive ;;
  }

  dimension: relationship_priority {
    type: number
    value_format: "0"
    sql: ${TABLE}.relationship_priority ;;
  }

  dimension_group: relationship_start {
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
    sql: ${TABLE}.relationship_start_date ;;
  }

  dimension: relationship_type {
    type: string
    sql: ${TABLE}.relationship_type ;;
  }

  dimension: relationship_type_raw_text {
    type: string
    sql: ${TABLE}.relationship_type_raw_text ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: system_type {
    type: string
    sql: ${TABLE}.system_type ;;
  }

  dimension: system_type_raw_text {
    type: string
    sql: ${TABLE}.system_type_raw_text ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
