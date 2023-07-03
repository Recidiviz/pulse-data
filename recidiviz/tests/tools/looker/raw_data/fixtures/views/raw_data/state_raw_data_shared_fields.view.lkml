# This file was automatically generated using a pulse-data script on 2000-06-30.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_view_generator.py`.

view: state_raw_data_shared_fields {


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
}
