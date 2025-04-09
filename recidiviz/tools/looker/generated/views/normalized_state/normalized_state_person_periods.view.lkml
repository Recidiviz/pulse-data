# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_person_periods {
  derived_table: {
    sql: 
    SELECT
        external_id AS period_id,
	'incarceration_period' AS period_type,
	person_id AS person_id,
	admission_date AS start_date,
	admission_reason AS start_reason,
	IFNULL(release_date, CURRENT_DATE('US/Eastern')) AS end_date,
	release_reason AS end_reason
      FROM normalized_state.state_incarceration_period
      UNION ALL
      SELECT
        external_id AS period_id,
	'supervision_period' AS period_type,
	person_id AS person_id,
	start_date AS start_date,
	admission_reason AS start_reason,
	IFNULL(termination_date, CURRENT_DATE('US/Eastern')) AS end_date,
	termination_reason AS end_reason
      FROM normalized_state.state_supervision_period
 ;;
  }

  dimension_group: end {
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
    sql: ${TABLE}.end_date ;;
  }

  dimension: end_reason {
    type: string
    sql: ${TABLE}.end_reason ;;
  }

  dimension: period_id {
    type: string
    sql: ${TABLE}.period_id ;;
  }

  dimension: period_type {
    type: string
    sql: ${TABLE}.period_type ;;
  }

  dimension: person_id {
    type: number
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    sql: CONCAT(${TABLE}.period_id, "_", ${TABLE}.period_type) ;;
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

  dimension: start_reason {
    type: string
    sql: ${TABLE}.start_reason ;;
  }
}
