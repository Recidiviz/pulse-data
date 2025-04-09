# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

view: us_ll_raw_data_shared_fields {
  extension: required


  dimension: file_id {
    description: "The ID of the file this row was extracted from"
    type: number
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.file_id
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  dimension: is_deleted {
    description: "Whether this row is inferred deleted via omission from more recent files"
    type: yesno
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.is_deleted
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  dimension_group: update_datetime {
    description: "The timestamp of the file this row was extracted from"
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
    sql: {% if us_ll_basicData.view_type._parameter_value == 'raw_data' %} ${TABLE}.update_datetime
      {% elsif us_ll_basicData.view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }
}
