# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/entity/state_dataset_lookml_writer.py`.

view: normalized_state_person {
  sql_table_name: normalized_state.state_person ;;

  dimension: actions {
    type: string
    sql: ${TABLE}.person_id ;;
    hidden: yes
    html: 
    <style>
       {

      }
    </style>
      <a
        href="/dashboards/recidiviz-staging::state_person?Person+ID={{ _filters['normalized_state_person.person_id'] }}&State+Code={{ _filters['normalized_state_person.state_code'] }}&External+ID={{ _filters['normalized_state_person_external_id.external_id'] }}&ID+Type={{ _filters['normalized_state_person_external_id.id_type'] }}"
        style="
          position: relative;
          display: inline-block;
          text-align: center;
          border: 1px solid #1890ff;
          text-decoration: none;
          color: #fff;
          background: #1890ff;
          text-shadow: 0 -1px 0 rgb(0 0 0 / 12%);
          box-shadow: 0 2px 0 rgb(0 0 0 / 5%);
          padding: 0 7px;
          border-radius: 3px;
        "
      >
        Switch to State Person
      </a>
 ;;
  }

  dimension_group: birthdate {
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
    sql: ${TABLE}.birthdate ;;
  }

  dimension: current_address {
    type: string
    sql: ${TABLE}.current_address ;;
  }

  dimension: current_email_address {
    type: string
    sql: ${TABLE}.current_email_address ;;
  }

  dimension: current_phone_number {
    type: string
    sql: ${TABLE}.current_phone_number ;;
  }

  dimension: full_name {
    type: string
    sql: ${TABLE}.full_name ;;
  }

  dimension: full_name_clean {
    type: string
    label: "Client Name"
    description: "Client's capitalized given names and surname"
    sql: CONCAT(
    INITCAP(JSON_EXTRACT_SCALAR(${full_name}, "$.given_names")),
    " ",
    INITCAP(JSON_EXTRACT_SCALAR(${full_name}, "$.surname"))
    ) ;;
  }

  dimension: gender {
    type: string
    sql: ${TABLE}.gender ;;
  }

  dimension: gender_raw_text {
    type: string
    sql: ${TABLE}.gender_raw_text ;;
  }

  dimension: person_id {
    type: number
    primary_key: yes
    value_format: "0"
    sql: ${TABLE}.person_id ;;
  }

  dimension: residency_status {
    type: string
    sql: ${TABLE}.residency_status ;;
  }

  dimension: residency_status_raw_text {
    type: string
    sql: ${TABLE}.residency_status_raw_text ;;
  }

  dimension: state_code {
    type: string
    sql: ${TABLE}.state_code ;;
  }

  measure: count {
    type: count
    drill_fields: [person_id, state_code, full_name]
  }
}
