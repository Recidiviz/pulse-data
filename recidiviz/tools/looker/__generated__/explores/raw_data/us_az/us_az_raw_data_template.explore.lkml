# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_az_raw_data_template {
  extension: required

  view_name: us_az_PERSON
  view_label: "us_az_PERSON"

  description: "Data pertaining to an individual in Arizona"
  group_label: "Raw State Data"
  label: "US_AZ Raw Data"
  join: us_az_DEMOGRAPHICS {
    sql_on: ${us_az_PERSON.PERSON_ID} = ${us_az_DEMOGRAPHICS.PERSON_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_DEMOGRAPHICS"
  }

  join: us_az_OCCUPANCY {
    sql_on: ${us_az_PERSON.PERSON_ID} = ${us_az_OCCUPANCY.PERSON_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_OCCUPANCY"
  }

  join: us_az_DPP_EPISODE {
    sql_on: ${us_az_PERSON.PERSON_ID} = ${us_az_DPP_EPISODE.PERSON_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_DPP_EPISODE"
  }

  join: us_az_DOC_EPISODE {
    sql_on: ${us_az_PERSON.PERSON_ID} = ${us_az_DOC_EPISODE.PERSON_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_DOC_EPISODE"
  }

  join: us_az_LOOKUPS {
    sql_on: ${us_az_PERSON.PERSON_TYPE_ID} = ${us_az_LOOKUPS.LOOKUP_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_LOOKUPS"
  }

  join: us_az_MEA_PROFILES {
    sql_on: ${us_az_PERSON.PERSON_ID} = ${us_az_MEA_PROFILES.USERID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_MEA_PROFILES"
  }

  join: us_az_RECIDIVIZ_REFERENCE_staff_id_override {
    sql_on: ${us_az_PERSON.PERSON_ID} = ${us_az_RECIDIVIZ_REFERENCE_staff_id_override.PERSON_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_az_RECIDIVIZ_REFERENCE_staff_id_override"
  }

}
