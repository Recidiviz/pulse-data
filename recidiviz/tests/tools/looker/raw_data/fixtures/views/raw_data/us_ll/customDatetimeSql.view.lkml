# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "../state_raw_data_shared_fields.view"
view: customDatetimeSql {
  extends: [
    state_raw_data_shared_fields
  ]
  sql_table_name: {% if view_type._parameter_value == 'raw_data' %} us_ll_raw_data.customDatetimeSql
    {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.customDatetimeSql_latest
    {% endif %} ;;

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(${file_id}, ${COL1}) ;;
  }

  dimension: COL1 {
    label: "COL1"
    type: string
    sql: ${TABLE}.COL1 ;;
    group_label: "Primary Key"
  }

  dimension_group: COL2 {
    description: "[DATE PARSED FROM COL2__raw]Datetime description here"
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    datatype: datetime
    sql: COALESCE(SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE(${TABLE}.COL2, r'\:\d\d\d.*', ''))) ;;
  }

  dimension: COL2__raw {
    label: "COL2__raw"
    type: string
    sql: ${TABLE}.COL2 ;;
    description: "Datetime description here"
  }

  measure: count {
    type: count
    drill_fields: [file_id, COL1]
  }
}
