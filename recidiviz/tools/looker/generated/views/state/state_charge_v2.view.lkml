# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_charge_v2 {
  sql_table_name: state.state_charge_v2 ;;

  dimension: attempted {
    type: yesno
    sql: ${TABLE}.attempted ;;
  }

  dimension: charge_notes {
    type: string
    sql: ${TABLE}.charge_notes ;;
  }

  dimension: charge_v2_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.charge_v2_id ;;
  }

  dimension: charging_entity {
    type: string
    sql: ${TABLE}.charging_entity ;;
  }

  dimension: classification_subtype {
    type: string
    sql: ${TABLE}.classification_subtype ;;
  }

  dimension: classification_type {
    type: string
    sql: ${TABLE}.classification_type ;;
  }

  dimension: classification_type_raw_text {
    type: string
    sql: ${TABLE}.classification_type_raw_text ;;
  }

  dimension: counts {
    type: number
    value_format: "0"
    sql: ${TABLE}.counts ;;
  }

  dimension: county_code {
    type: string
    sql: ${TABLE}.county_code ;;
  }

  dimension_group: date_charged {
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
    sql: ${TABLE}.date_charged ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: is_controlling {
    type: yesno
    sql: ${TABLE}.is_controlling ;;
  }

  dimension: is_drug {
    type: yesno
    sql: ${TABLE}.is_drug ;;
  }

  dimension: is_sex_offense {
    type: yesno
    sql: ${TABLE}.is_sex_offense ;;
  }

  dimension: is_violent {
    type: yesno
    sql: ${TABLE}.is_violent ;;
  }

  dimension: judge_external_id {
    type: string
    sql: ${TABLE}.judge_external_id ;;
  }

  dimension: judge_full_name {
    type: string
    sql: ${TABLE}.judge_full_name ;;
  }

  dimension: judicial_district_code {
    type: string
    sql: ${TABLE}.judicial_district_code ;;
  }

  dimension: ncic_code {
    type: string
    sql: ${TABLE}.ncic_code ;;
  }

  dimension_group: offense {
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
    sql: ${TABLE}.offense_date ;;
  }

  dimension: offense_type {
    type: string
    sql: ${TABLE}.offense_type ;;
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

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: status_raw_text {
    type: string
    sql: ${TABLE}.status_raw_text ;;
  }

  dimension: statute {
    type: string
    sql: ${TABLE}.statute ;;
  }

  measure: average_counts {
    type: average
    sql: ${counts} ;;
  }

  measure: count {
    type: count
    drill_fields: [charge_v2_id, description]
  }

  measure: total_counts {
    type: sum
    sql: ${counts} ;;
  }
}
