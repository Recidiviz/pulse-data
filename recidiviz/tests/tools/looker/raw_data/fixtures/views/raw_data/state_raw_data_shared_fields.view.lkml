# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

view: state_raw_data_shared_fields {
  extension: required


  parameter: view_type {
    type: unquoted
    description: "Used to select whether the view has the most recent version (raw data up to date views) or all data"
    allowed_value: {
      label: "Raw Data"
      value: "raw_data"
    }
    allowed_value: {
      label: "Raw Data Up To Date Views"
      value: "raw_data_up_to_date_views"
    }
    default_value: "raw_data_up_to_date_views"
  }

  dimension: file_id {
    description: "The ID of the file this row was extracted from"
    type: number
    sql: {% if view_type._parameter_value == 'raw_data' %} ${TABLE}.file_id
      {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  dimension: is_deleted {
    description: "Whether this row is inferred deleted via omission from more recent files"
    type: yesno
    sql: {% if view_type._parameter_value == 'raw_data' %} ${TABLE}.is_deleted
      {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }

  dimension_group: update_datetime {
    description: "The timestamp of the file this row was extracted from"
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
    sql: {% if view_type._parameter_value == 'raw_data' %} ${TABLE}.update_datetime
      {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} NULL
      {% endif %} ;;
  }
}
