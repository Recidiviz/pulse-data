# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/top_level_generators/document_stores_lookml_generator.py`.

explore: us_co_supervision_contact_notes_documents_template {
  extension: required

  view_name: us_co_supervision_contact_notes_metadata
  view_label: "Supervision Contact Notes Metadata"

  label: "US_CO Supervision Contact Notes"
  join: us_co_supervision_contact_notes_document_contents {
    type: left_outer
    view_label: "Document Text"
    relationship: many_to_one
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_supervision_contact_notes_document_contents.document_contents_id};;
  }

  join: state_person_external_id {
    type: left_outer
    view_label: "Person"
    fields: [state_person_external_id.external_id, state_person_external_id.id_type]
    relationship: many_to_one
    sql_on: ${us_co_supervision_contact_notes_metadata.person_external_id} = ${state_person_external_id.external_id}
            AND ${us_co_supervision_contact_notes_metadata.person_external_id_type} = ${state_person_external_id.id_type};;
  }

  join: state_person {
    type: left_outer
    view_label: "Person"
    relationship: many_to_one
    sql_on: ${state_person_external_id.person_id} = ${state_person.person_id};;
  }

  join: us_co_case_note_employment_info {
    type: left_outer
    view_label: "Case Note Employment Info"
    relationship: many_to_one
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_case_note_employment_info.document_contents_id};;
  }

  join: us_co_case_note_employment_info_employers {
    type: left_outer
    view_label: "Case Note Employment Info: Employers"
    relationship: one_to_many
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_case_note_employment_info_employers.document_contents_id};;
  }

  join: us_co_case_note_employment_info_employment_changes {
    type: left_outer
    view_label: "Case Note Employment Info: Employment Changes"
    relationship: one_to_many
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_case_note_employment_info_employment_changes.document_contents_id};;
  }

  join: us_co_case_note_housing_info {
    type: left_outer
    view_label: "Case Note Housing Info"
    relationship: many_to_one
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_case_note_housing_info.document_contents_id};;
  }

  join: us_co_case_note_payment_info {
    type: left_outer
    view_label: "Case Note Payment Info"
    relationship: many_to_one
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_case_note_payment_info.document_contents_id};;
  }

  join: us_co_case_note_payment_info_payments {
    type: left_outer
    view_label: "Case Note Payment Info: Payments"
    relationship: one_to_many
    sql_on: ${us_co_supervision_contact_notes_metadata.document_contents_id} = ${us_co_case_note_payment_info_payments.document_contents_id};;
  }

}
