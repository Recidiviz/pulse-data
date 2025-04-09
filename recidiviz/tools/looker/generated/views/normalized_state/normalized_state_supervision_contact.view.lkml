# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_supervision_contact {
  sql_table_name: normalized_state.state_supervision_contact ;;

  dimension_group: contact {
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
    sql: ${TABLE}.contact_date ;;
  }

  dimension: contact_method {
    type: string
    sql: ${TABLE}.contact_method ;;
  }

  dimension: contact_method_raw_text {
    type: string
    sql: ${TABLE}.contact_method_raw_text ;;
  }

  dimension: contact_reason {
    type: string
    sql: ${TABLE}.contact_reason ;;
  }

  dimension: contact_reason_raw_text {
    type: string
    sql: ${TABLE}.contact_reason_raw_text ;;
  }

  dimension: contact_type {
    type: string
    sql: ${TABLE}.contact_type ;;
  }

  dimension: contact_type_raw_text {
    type: string
    sql: ${TABLE}.contact_type_raw_text ;;
  }

  dimension: contacting_staff_external_id {
    type: string
    sql: ${TABLE}.contacting_staff_external_id ;;
  }

  dimension: contacting_staff_external_id_type {
    type: string
    sql: ${TABLE}.contacting_staff_external_id_type ;;
  }

  dimension: contacting_staff_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.contacting_staff_id ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: location {
    type: string
    sql: ${TABLE}.location ;;
  }

  dimension: location_raw_text {
    type: string
    sql: ${TABLE}.location_raw_text ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: resulted_in_arrest {
    type: yesno
    sql: ${TABLE}.resulted_in_arrest ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: status_raw_text {
    type: string
    sql: ${TABLE}.status_raw_text ;;
  }

  dimension: supervision_contact_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_contact_id ;;
  }

  dimension: supervision_contact_metadata {
    type: string
    sql: ${TABLE}.supervision_contact_metadata ;;
  }

  dimension: verified_employment {
    type: yesno
    sql: ${TABLE}.verified_employment ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
