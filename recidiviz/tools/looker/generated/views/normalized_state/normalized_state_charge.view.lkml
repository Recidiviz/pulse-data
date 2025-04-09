# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_charge {
  sql_table_name: normalized_state.state_charge ;;

  dimension: attempted {
    type: yesno
    sql: ${TABLE}.attempted ;;
  }

  dimension: charge_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.charge_id ;;
  }

  dimension: charge_notes {
    type: string
    sql: ${TABLE}.charge_notes ;;
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

  dimension: description_external {
    type: string
    sql: ${TABLE}.description_external ;;
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
    hidden: yes
  }

  dimension: is_drug_external {
    type: yesno
    sql: ${TABLE}.is_drug_external ;;
  }

  dimension: is_sex_offense {
    hidden: yes
  }

  dimension: is_sex_offense_external {
    type: yesno
    sql: ${TABLE}.is_sex_offense_external ;;
  }

  dimension: is_violent {
    hidden: yes
  }

  dimension: is_violent_external {
    type: yesno
    sql: ${TABLE}.is_violent_external ;;
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

  dimension: ncic_category_external {
    type: string
    sql: ${TABLE}.ncic_category_external ;;
  }

  dimension: ncic_code {
    hidden: yes
  }

  dimension: ncic_code_external {
    type: string
    sql: ${TABLE}.ncic_code_external ;;
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

  measure: count {
    type: count
    drill_fields: []
  }
}
