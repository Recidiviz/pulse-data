# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_program_assignment {
  sql_table_name: normalized_state.state_program_assignment ;;

  dimension_group: discharge {
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
    sql: ${TABLE}.discharge_date ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: participation_status {
    type: string
    sql: ${TABLE}.participation_status ;;
  }

  dimension: participation_status_raw_text {
    type: string
    sql: ${TABLE}.participation_status_raw_text ;;
  }

  dimension: person_id {
    type: number
    hidden: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: program_assignment_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.program_assignment_id ;;
  }

  dimension: program_id {
    type: string
    sql: ${TABLE}.program_id ;;
  }

  dimension: program_location_id {
    type: string
    sql: ${TABLE}.program_location_id ;;
  }

  dimension_group: referral {
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
    sql: ${TABLE}.referral_date ;;
  }

  dimension: referral_metadata {
    type: string
    sql: ${TABLE}.referral_metadata ;;
  }

  dimension: referring_staff_external_id {
    type: string
    sql: ${TABLE}.referring_staff_external_id ;;
  }

  dimension: referring_staff_external_id_type {
    type: string
    sql: ${TABLE}.referring_staff_external_id_type ;;
  }

  dimension: referring_staff_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.referring_staff_id ;;
  }

  dimension: sequence_num {
    type: number
    value_format: "0"
    sql: ${TABLE}.sequence_num ;;
  }

  dimension_group: start {
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
    sql: ${TABLE}.start_date ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: referrals_array {
    type: string
    description: "List in string form of all program referral dates and parenthesized program_id's"
    sql: ARRAY_TO_STRING(ARRAY_AGG(
      DISTINCT CONCAT(CAST(${TABLE}.referral_date AS STRING), " (", ${TABLE}.program_id, ")")
      ORDER BY CONCAT(CAST(${TABLE}.referral_date AS STRING), " (", ${TABLE}.program_id, ")")
    ), ";\r\n") ;;
    html: <div style="white-space:pre">{{ value }}</div> ;;
  }
}
