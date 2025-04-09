# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: state_supervision_violation {
  sql_table_name: state.state_supervision_violation ;;

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: is_sex_offense {
    type: yesno
    sql: ${TABLE}.is_sex_offense ;;
  }

  dimension: is_violent {
    type: yesno
    sql: ${TABLE}.is_violent ;;
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

  dimension: supervision_violation_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.supervision_violation_id ;;
  }

  dimension_group: violation {
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
    sql: ${TABLE}.violation_date ;;
  }

  dimension: violation_metadata {
    type: string
    sql: ${TABLE}.violation_metadata ;;
  }

  dimension: violation_severity {
    type: string
    sql: ${TABLE}.violation_severity ;;
  }

  dimension: violation_severity_raw_text {
    type: string
    sql: ${TABLE}.violation_severity_raw_text ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
