# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "../state_raw_data_shared_fields.view"
view: datetimeNoParsers {
  extends: [
    state_raw_data_shared_fields
  ]
  sql_table_name: {% if view_type._parameter_value == 'raw_data' %} us_ll_raw_data.datetimeNoParsers
    {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.datetimeNoParsers_latest
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
    description: "[DATE PARSED FROM COL2__raw]"
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
    sql: NULL ;;
  }

  dimension: COL2__raw {
    label: "COL2__raw"
    type: string
    sql: ${TABLE}.COL2 ;;
  }

  measure: count {
    type: count
    drill_fields: [file_id, COL1]
  }
}
