# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

include: "../state_raw_data_shared_fields.view"
view: tagWeDoNotIngest {
  extends: [
    state_raw_data_shared_fields
  ]
  sql_table_name: {% if view_type._parameter_value == 'raw_data' %} us_xx_raw_data.tagWeDoNotIngest
    {% elsif view_type._parameter_value == 'raw_data_up_to_date_views' %} us_xx_raw_data_up_to_date_views.tagWeDoNotIngest_latest
    {% endif %} ;;
}
