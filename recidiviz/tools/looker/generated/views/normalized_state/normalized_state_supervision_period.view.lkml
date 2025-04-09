# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_supervision_period {
  sql_table_name: normalized_state.state_supervision_period ;;

  dimension: admission_reason {
    type: string
    sql: ${TABLE}.admission_reason ;;
  }

  dimension: admission_reason_raw_text {
    type: string
    sql: ${TABLE}.admission_reason_raw_text ;;
  }

  dimension: conditions {
    type: string
    sql: ${TABLE}.conditions ;;
  }

  dimension: county_code {
    type: string
    sql: ${TABLE}.county_code ;;
  }

  dimension: custodial_authority {
    type: string
    sql: ${TABLE}.custodial_authority ;;
  }

  dimension: custodial_authority_raw_text {
    type: string
    sql: ${TABLE}.custodial_authority_raw_text ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: person_id {
    type: number
    html: 
      {% if normalized_state_supervision_period.termination_date._value %}
        <font >{{ rendered_value }}</font>
      {% else %}
        <font >❇️ {{ rendered_value }}</font>
      {% endif %} ;;
    value_format: "0"
    sql: ${TABLE}.person_id ;;
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

  dimension: supervising_officer_staff_external_id {
    type: string
    sql: ${TABLE}.supervising_officer_staff_external_id ;;
  }

  dimension: supervising_officer_staff_external_id_type {
    type: string
    sql: ${TABLE}.supervising_officer_staff_external_id_type ;;
  }

  dimension: supervising_officer_staff_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.supervising_officer_staff_id ;;
  }

  dimension: supervision_level {
    type: string
    sql: ${TABLE}.supervision_level ;;
  }

  dimension: supervision_level_raw_text {
    type: string
    sql: ${TABLE}.supervision_level_raw_text ;;
  }

  dimension: supervision_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_period_id ;;
  }

  dimension: supervision_period_metadata {
    type: string
    sql: ${TABLE}.supervision_period_metadata ;;
  }

  dimension: supervision_site {
    type: string
    sql: ${TABLE}.supervision_site ;;
  }

  dimension: supervision_type {
    type: string
    sql: ${TABLE}.supervision_type ;;
  }

  dimension: supervision_type_raw_text {
    type: string
    sql: ${TABLE}.supervision_type_raw_text ;;
  }

  dimension_group: termination {
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
    sql: ${TABLE}.termination_date ;;
  }

  dimension: termination_reason {
    type: string
    sql: ${TABLE}.termination_reason ;;
  }

  dimension: termination_reason_raw_text {
    type: string
    sql: ${TABLE}.termination_reason_raw_text ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
