# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "us_ll_raw_data_shared_fields.view"
view: us_ll_customDatetimeSql {
  extends: [
    us_ll_raw_data_shared_fields
  ]
  sql_table_name: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} us_ll_raw_data.customDatetimeSql
    {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} us_ll_raw_data_up_to_date_views.customDatetimeSql_latest
    {% endif %} ;;

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(IFNULL(CAST(${file_id} AS STRING), ""), IFNULL(CAST(${COL1} AS STRING), "")) ;;
  }

  dimension: COL1 {
    label: "COL1"
    type: string
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.COL1
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
    group_label: "Primary Key"
  }

  dimension_group: COL2 {
    description: "[DATE PARSED FROM COL2__raw]Datetime description here"
    type: time
    timeframes: [
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    datatype: datetime
    sql: COALESCE(SAFE.PARSE_DATETIME('%b %e %Y %H:%M:%S', REGEXP_REPLACE(${TABLE}.COL2, r'\:\d\d\d.*', ''))) ;;
  }

  dimension: COL2__raw {
    label: "COL2__raw"
    type: string
    description: "Datetime description here"
    sql: ${TABLE}.COL2 ;;
  }

  measure: count {
    type: count
    drill_fields: [file_id, COL1]
  }
}
