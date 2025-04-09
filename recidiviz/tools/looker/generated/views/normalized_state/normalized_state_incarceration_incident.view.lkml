# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_incarceration_incident {
  sql_table_name: normalized_state.state_incarceration_incident ;;

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: facility {
    type: string
    sql: ${TABLE}.facility ;;
  }

  dimension: incarceration_incident_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.incarceration_incident_id ;;
  }

  dimension_group: incident {
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
    sql: ${TABLE}.incident_date ;;
  }

  dimension: incident_details {
    type: string
    sql: ${TABLE}.incident_details ;;
  }

  dimension: incident_metadata {
    type: string
    sql: ${TABLE}.incident_metadata ;;
  }

  dimension: incident_severity {
    type: string
    sql: ${TABLE}.incident_severity ;;
  }

  dimension: incident_severity_raw_text {
    type: string
    sql: ${TABLE}.incident_severity_raw_text ;;
  }

  dimension: incident_type {
    type: string
    sql: ${TABLE}.incident_type ;;
  }

  dimension: incident_type_raw_text {
    type: string
    sql: ${TABLE}.incident_type_raw_text ;;
  }

  dimension: location_within_facility {
    type: string
    sql: ${TABLE}.location_within_facility ;;
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

  measure: count {
    type: count
    drill_fields: []
  }
}
