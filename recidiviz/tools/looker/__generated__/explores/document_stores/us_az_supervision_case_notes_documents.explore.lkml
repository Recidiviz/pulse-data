# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/document_stores_lookml_generator.py`.

explore: us_az_supervision_case_notes_documents_template {
  extension: required

  view_name: us_az_supervision_case_notes_metadata
  view_label: "Supervision Case Notes Metadata"

  label: "US_AZ Supervision Case Notes"
  join: us_az_supervision_case_notes_document_contents {
    type: left_outer
    view_label: "Document Text"
    relationship: many_to_one
    sql_on: ${us_az_supervision_case_notes_metadata.document_contents_id} = ${us_az_supervision_case_notes_document_contents.document_contents_id};;
  }

  join: state_person_external_id {
    type: left_outer
    view_label: "Person"
    fields: [state_person_external_id.external_id, state_person_external_id.id_type]
    relationship: many_to_one
    sql_on: ${us_az_supervision_case_notes_metadata.person_external_id} = ${state_person_external_id.external_id}
            AND ${us_az_supervision_case_notes_metadata.person_external_id_type} = ${state_person_external_id.id_type};;
  }

  join: state_person {
    type: left_outer
    view_label: "Person"
    relationship: many_to_one
    sql_on: ${state_person_external_id.person_id} = ${state_person.person_id};;
  }

}
