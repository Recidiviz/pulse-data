# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_incarceration_period {
  sql_table_name: normalized_state.state_incarceration_period ;;

  dimension_group: admission {
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
    sql: ${TABLE}.admission_date ;;
  }

  dimension: admission_reason {
    type: string
    sql: ${TABLE}.admission_reason ;;
  }

  dimension: admission_reason_raw_text {
    type: string
    sql: ${TABLE}.admission_reason_raw_text ;;
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

  dimension: custody_level {
    type: string
    sql: ${TABLE}.custody_level ;;
  }

  dimension: custody_level_raw_text {
    type: string
    sql: ${TABLE}.custody_level_raw_text ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: facility {
    type: string
    sql: ${TABLE}.facility ;;
  }

  dimension: housing_unit {
    type: string
    sql: ${TABLE}.housing_unit ;;
  }

  dimension: housing_unit_category {
    type: string
    sql: ${TABLE}.housing_unit_category ;;
  }

  dimension: housing_unit_category_raw_text {
    type: string
    sql: ${TABLE}.housing_unit_category_raw_text ;;
  }

  dimension: housing_unit_type {
    type: string
    sql: ${TABLE}.housing_unit_type ;;
  }

  dimension: housing_unit_type_raw_text {
    type: string
    sql: ${TABLE}.housing_unit_type_raw_text ;;
  }

  dimension: incarceration_admission_violation_type {
    type: string
    sql: ${TABLE}.incarceration_admission_violation_type ;;
  }

  dimension: incarceration_period_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.incarceration_period_id ;;
  }

  dimension: incarceration_type {
    type: string
    sql: ${TABLE}.incarceration_type ;;
  }

  dimension: incarceration_type_raw_text {
    type: string
    sql: ${TABLE}.incarceration_type_raw_text ;;
  }

  dimension: person_id {
    type: number
    html: 
      {% if normalized_state_incarceration_period.release_date._value %}
        <font >{{ rendered_value }}</font>
      {% else %}
        <font >❇️ {{ rendered_value }}</font>
      {% endif %} ;;
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: purpose_for_incarceration_subtype {
    type: string
    sql: ${TABLE}.purpose_for_incarceration_subtype ;;
  }

  dimension_group: release {
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
    sql: ${TABLE}.release_date ;;
  }

  dimension: release_reason {
    type: string
    sql: ${TABLE}.release_reason ;;
  }

  dimension: release_reason_raw_text {
    type: string
    sql: ${TABLE}.release_reason_raw_text ;;
  }

  dimension: sequence_num {
    type: number
    value_format: "0"
    sql: ${TABLE}.sequence_num ;;
  }

  dimension: specialized_purpose_for_incarceration {
    type: string
    sql: ${TABLE}.specialized_purpose_for_incarceration ;;
  }

  dimension: specialized_purpose_for_incarceration_raw_text {
    type: string
    sql: ${TABLE}.specialized_purpose_for_incarceration_raw_text ;;
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
