# This file was automatically generated using a pulse-data script.
# To regenerate, see `recidiviz/tools/looker/raw_data/person_details_explore_generator.py`.

explore: us_nd_raw_data_template {
  extension: required

  view_name: us_nd_docstars_offenders
  view_label: "us_nd_docstars_offenders"

  description: "Data pertaining to an individual in North Dakota"
  group_label: "Raw State Data"
  label: "US_ND Raw Data"
  join: us_nd_docstars_contacts {
    sql_on: ${us_nd_docstars_offenders.SID} = ${us_nd_docstars_contacts.SID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_docstars_contacts"
  }

  join: us_nd_docstars_ftr_episode {
    sql_on: ${us_nd_docstars_offenders.SID} = ${us_nd_docstars_ftr_episode.SID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_docstars_ftr_episode"
  }

  join: us_nd_docstars_lsi_chronology {
    sql_on: ${us_nd_docstars_offenders.SID} = ${us_nd_docstars_lsi_chronology.SID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_docstars_lsi_chronology"
  }

  join: us_nd_docstars_offendercasestable {
    sql_on: ${us_nd_docstars_offenders.SID} = ${us_nd_docstars_offendercasestable.SID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_docstars_offendercasestable"
  }

  join: us_nd_docstars_offensestable {
    sql_on: ${us_nd_docstars_offenders.SID} = ${us_nd_docstars_offensestable.SID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_docstars_offensestable"
  }

  join: us_nd_elite_offenderidentifier {
    sql_on: ${us_nd_docstars_offenders.SID} = ${us_nd_elite_offenderidentifier.IDENTIFIER} AND ${us_nd_elite_offenderidentifier.IDENTIFIER_TYPE} = 'SID';;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offenderidentifier"
  }

  join: us_nd_elite_offenders {
    sql_on: REPLACE(REPLACE(${us_nd_elite_offenders.ROOT_OFFENDER_ID},',',''), '.00', '') = REPLACE(REPLACE(${us_nd_elite_offenderidentifier.ROOT_OFFENDER_ID},',',''), '.00', '');;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offenders"
  }

  join: us_nd_elite_alias {
    sql_on: REPLACE(REPLACE(${us_nd_elite_offenders.ROOT_OFFENDER_ID},',',''), '.00', '') = REPLACE(REPLACE(${us_nd_elite_alias.ROOT_OFFENDER_ID},',',''), '.00', '');;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_alias"
  }

  join: us_nd_elite_offenderbookingstable {
    sql_on: REPLACE(REPLACE(${us_nd_elite_offenders.ROOT_OFFENDER_ID},',',''), '.00', '') = REPLACE(REPLACE(${us_nd_elite_offenderbookingstable.ROOT_OFFENDER_ID},',',''), '.00', '');;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offenderbookingstable"
  }

  join: us_nd_elite_offense_in_custody_and_pos_report_data {
    sql_on: REPLACE(REPLACE(${us_nd_elite_offenders.ROOT_OFFENDER_ID},',',''), '.00', '') = REPLACE(REPLACE(${us_nd_elite_offense_in_custody_and_pos_report_data.ROOT_OFFENDER_ID},',',''), '.00', '');;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offense_in_custody_and_pos_report_data"
  }

  join: us_nd_recidiviz_elite_offender_alerts {
    sql_on: REPLACE(REPLACE(${us_nd_elite_offenders.ROOT_OFFENDER_ID},',',''), '.00', '') = REPLACE(REPLACE(${us_nd_recidiviz_elite_offender_alerts.ROOT_OFFENDER_ID},',',''), '.00', '');;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_recidiviz_elite_offender_alerts"
  }

  join: us_nd_elite_bedassignmenthistory {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_bedassignmenthistory.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_bedassignmenthistory"
  }

  join: us_nd_elite_externalmovements {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_externalmovements.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_externalmovements"
  }

  join: us_nd_elite_institutionalactivities {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_institutionalactivities.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_institutionalactivities"
  }

  join: us_nd_elite_offendersentenceaggs {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_offendersentenceaggs.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offendersentenceaggs"
  }

  join: us_nd_elite_offendersentences {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_offendersentences.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offendersentences"
  }

  join: us_nd_elite_offendersentenceterms {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_offendersentenceterms.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_offendersentenceterms"
  }

  join: us_nd_elite_orderstable {
    sql_on: ${us_nd_elite_offenderbookingstable.OFFENDER_BOOK_ID} = ${us_nd_elite_orderstable.OFFENDER_BOOK_ID};;
    type: full_outer
    relationship: many_to_many
    view_label: "us_nd_elite_orderstable"
  }

}
